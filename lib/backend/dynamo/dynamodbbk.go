/*
Copyright 2015-2018 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package dynamo

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/defaults"
	"github.com/gravitational/teleport/lib/utils"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"
)

// DynamoConfig structure represents DynamoDB confniguration as appears in `storage` section
// of Teleport YAML
type DynamoConfig struct {
	// Region is where DynamoDB Table will be used to store k/v
	Region string `json:"region,omitempty"`
	// AWS AccessKey used to authenticate DynamoDB queries (prefer IAM role instead of hardcoded value)
	AccessKey string `json:"access_key,omitempty"`
	// AWS SecretKey used to authenticate DynamoDB queries (prefer IAM role instead of hardcoded value)
	SecretKey string `json:"secret_key,omitempty"`
	// Tablename where to store K/V in DynamoDB
	Tablename string `json:"table_name,omitempty"`
	// ReadCapacityUnits is Dynamodb read capacity units
	ReadCapacityUnits int64 `json:"read_capacity_units"`
	// WriteCapacityUnits is Dynamodb write capacity units
	WriteCapacityUnits int64 `json:"write_capacity_units"`
	// BufferSize is a default buffer size
	// used to pull events
	BufferSize int `json:"buffer_size,omitempty"`
	// PollStreamPeriod is a polling period for event stream
	PollStreamPeriod time.Duration `json:"poll_stream_period,omitempty"`
}

// CheckAndSetDefaults is a helper returns an error if the supplied configuration
// is not enough to connect to DynamoDB
func (cfg *DynamoConfig) CheckAndSetDefaults() error {
	// table is not configured?
	if cfg.Tablename == "" {
		return trace.BadParameter("DynamoDB: table_name is not specified")
	}
	if cfg.ReadCapacityUnits == 0 {
		cfg.ReadCapacityUnits = DefaultReadCapacityUnits
	}
	if cfg.WriteCapacityUnits == 0 {
		cfg.WriteCapacityUnits = DefaultWriteCapacityUnits
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = backend.DefaultBufferSize
	}
	if cfg.PollStreamPeriod == 0 {
		cfg.PollStreamPeriod = backend.DefaultPollStreamPeriod
	}
	return nil
}

// DynamoDBBackend struct
type DynamoDBBackend struct {
	*log.Entry
	DynamoConfig
	svc              *dynamodb.DynamoDB
	streams          *dynamodbstreams.DynamoDBStreams
	clock            clockwork.Clock
	buf              *backend.CircularBuffer
	ctx              context.Context
	cancel           context.CancelFunc
	watchStarted     context.Context
	signalWatchStart context.CancelFunc
}

type record struct {
	HashKey   string
	FullPath  string
	Value     []byte
	Timestamp int64
	Expires   *int64 `json:"Expires,omitempty"`
	ID        int64
}

type keyLookup struct {
	HashKey  string
	FullPath string
}

const (
	// hashKey is actually the name of the partition. This backend
	// places all objects in the same DynamoDB partition
	hashKey = "teleport"

	// obsolete schema key. if a table contains "Key" column it means
	// such table needs to be migrated
	oldPathAttr = "Key"

	// BackendName is the name of this backend
	BackendName = "dynamodb"

	// ttlKey is a key used for TTL specification
	ttlKey = "Expires"

	// DefaultReadCapacityUnits specifies default value for read capacity units
	DefaultReadCapacityUnits = 10

	// DefaultWriteCapacityUnits specifies default value for write capacity units
	DefaultWriteCapacityUnits = 10

	// fullPathKey is a name of the full path key
	fullPathKey = "FullPath"

	// hashKeyKey is a name of the hash key
	hashKeyKey = "HashKey"
)

// GetName() is a part of backend API and it returns DynamoDB backend type
// as it appears in `storage/type` section of Teleport YAML
func GetName() string {
	return BackendName
}

// keep this here to test interface conformance
var _ backend.Backend = &DynamoDBBackend{}

// New returns new instance of DynamoDB backend.
// It's an implementation of backend API's NewFunc
func New(ctx context.Context, params backend.Params) (*DynamoDBBackend, error) {
	l := log.WithFields(log.Fields{trace.Component: BackendName})

	var cfg *DynamoConfig
	err := utils.ObjectToStruct(params, &cfg)
	if err != nil {
		return nil, trace.BadParameter("DynamoDB configuration is invalid: %v", err)
	}

	l.Infof("Initializing backend. Table: %q, poll streams every %v.", cfg.Tablename, cfg.PollStreamPeriod)

	defer l.Debug("AWS session is created.")

	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}

	buf, err := backend.NewCircularBuffer(ctx, cfg.BufferSize)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	closeCtx, cancel := context.WithCancel(ctx)
	watchStarted, signalWatchStart := context.WithCancel(ctx)
	b := &DynamoDBBackend{
		Entry:            l,
		DynamoConfig:     *cfg,
		clock:            clockwork.NewRealClock(),
		buf:              buf,
		ctx:              closeCtx,
		cancel:           cancel,
		watchStarted:     watchStarted,
		signalWatchStart: signalWatchStart,
	}
	// create an AWS session using default SDK behavior, i.e. it will interpret
	// the environment and ~/.aws directory just like an AWS CLI tool would:
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	// override the default environment (region + credentials) with the values
	// from the YAML file:
	if cfg.Region != "" {
		sess.Config.Region = aws.String(cfg.Region)
	}
	if cfg.AccessKey != "" || cfg.SecretKey != "" {
		creds := credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, "")
		sess.Config.Credentials = creds
	}

	// Increase the size of the connection pool. This substantially improves the
	// performance of Teleport under load as it reduces the number of TLS
	// handshakes performed.
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        defaults.HTTPMaxIdleConns,
			MaxIdleConnsPerHost: defaults.HTTPMaxIdleConnsPerHost,
		},
	}
	sess.Config.HTTPClient = httpClient

	// create DynamoDB service:
	b.svc = dynamodb.New(sess)
	b.streams = dynamodbstreams.New(sess)

	// check if the table exists?
	ts, err := b.getTableStatus(ctx, b.Tablename)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	switch ts {
	case tableStatusOK:
		break
	case tableStatusMissing:
		err = b.createTable(ctx, b.Tablename, fullPathKey)
	case tableStatusNeedsMigration:
		return nil, trace.BadParameter("unsupported schema")
	}
	if err != nil {
		return nil, trace.Wrap(err)
	}
	err = b.turnOnTimeToLive(ctx)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	err = b.turnOnStreams(ctx)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	go b.asyncPollStreams(ctx)

	// Wrap backend in a input sanitizer and return it.
	return b, nil
}

// Create creates item if it does not exist
func (b *DynamoDBBackend) Create(ctx context.Context, item backend.Item) (*backend.Lease, error) {
	err := b.create(ctx, item, modeCreate)
	if trace.IsCompareFailed(err) {
		err = trace.AlreadyExists(err.Error())
	}
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return b.newLease(item), nil
}

// Put puts value into backend (creates if it does not
// exists, updates it otherwise)
func (b *DynamoDBBackend) Put(ctx context.Context, item backend.Item) (*backend.Lease, error) {
	err := b.create(ctx, item, modePut)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return b.newLease(item), nil
}

// Update updates value in the backend
func (b *DynamoDBBackend) Update(ctx context.Context, item backend.Item) (*backend.Lease, error) {
	err := b.create(ctx, item, modeUpdate)
	if trace.IsCompareFailed(err) {
		err = trace.NotFound(err.Error())
	}
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return b.newLease(item), nil
}

// GetRange returns range of elements
func (b *DynamoDBBackend) GetRange(ctx context.Context, startKey []byte, endKey []byte, limit int) (*backend.GetResult, error) {
	if len(startKey) == 0 {
		return nil, trace.BadParameter("missing parameter startKey")
	}
	if len(endKey) == 0 {
		return nil, trace.BadParameter("missing parameter endKey")
	}
	result, err := b.getRecords(ctx, string(startKey), string(endKey), limit)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	values := make([]backend.Item, len(result.records))
	for i, r := range result.records {
		values[i] = backend.Item{
			Key:   []byte(r.FullPath),
			Value: r.Value,
		}
		if r.Expires != nil {
			values[i].Expires = time.Unix(*r.Expires, 0).UTC()
		}
	}
	return &backend.GetResult{Items: values}, nil
}

// DeleteRange deletes range of items with keys between startKey and endKey
func (b *DynamoDBBackend) DeleteRange(ctx context.Context, startKey, endKey []byte) error {
	if len(startKey) == 0 {
		return trace.BadParameter("missing parameter startKey")
	}
	if len(endKey) == 0 {
		return trace.BadParameter("missing parameter endKey")
	}
	// keep fetching and deleting until no records left,
	// keep the very large limit, just in case if someone else
	// keeps adding records
	for i := 0; i < backend.DefaultLargeLimit/100; i++ {
		result, err := b.getRecords(ctx, string(startKey), string(endKey), 100)
		if err != nil {
			return trace.Wrap(err)
		}
		if len(result.records) == 0 {
			return nil
		}
		requests := make([]*dynamodb.WriteRequest, 0, len(result.records))
		for _, record := range result.records {
			requests = append(requests, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]*dynamodb.AttributeValue{
						hashKeyKey: {
							S: aws.String(hashKey),
						},
						fullPathKey: {
							S: aws.String(record.FullPath),
						},
					},
				},
			})
		}
		input := dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				b.Tablename: requests,
			},
		}

		if _, err = b.svc.BatchWriteItemWithContext(ctx, &input); err != nil {
			return trace.Wrap(err)
		}
	}
	return trace.ConnectionProblem(nil, "not all items deleted, too many requests")
}

// Get returns a single item or not found error
func (b *DynamoDBBackend) Get(ctx context.Context, key []byte) (*backend.Item, error) {
	r, err := b.getKey(ctx, key)
	if err != nil {
		return nil, err
	}
	item := &backend.Item{
		Key:   []byte(r.FullPath),
		Value: r.Value,
		ID:    r.ID,
	}
	if r.Expires != nil {
		item.Expires = time.Unix(*r.Expires, 0)
	}
	return item, nil
}

// CompareAndSwap compares and swap values in atomic operation
// CompareAndSwap compares item with existing item
// and replaces is with replaceWith item
func (b *DynamoDBBackend) CompareAndSwap(ctx context.Context, expected backend.Item, replaceWith backend.Item) (*backend.Lease, error) {
	if len(expected.Key) == 0 {
		return nil, trace.BadParameter("missing parameter Key")
	}
	if len(replaceWith.Key) == 0 {
		return nil, trace.BadParameter("missing parameter Key")
	}
	if bytes.Compare(expected.Key, replaceWith.Key) != 0 {
		return nil, trace.BadParameter("expected and replaceWith keys should match")
	}
	r := record{
		HashKey:   hashKey,
		FullPath:  string(replaceWith.Key),
		Value:     replaceWith.Value,
		Timestamp: time.Now().UTC().Unix(),
		ID:        time.Now().UTC().UnixNano(),
	}
	if !replaceWith.Expires.IsZero() {
		r.Expires = aws.Int64(replaceWith.Expires.UTC().Unix())
	}
	av, err := dynamodbattribute.MarshalMap(r)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	input := dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(b.Tablename),
	}
	input.SetConditionExpression("#v = :prev")
	input.SetExpressionAttributeNames(map[string]*string{
		"#v": aws.String("Value"),
	})
	input.SetExpressionAttributeValues(map[string]*dynamodb.AttributeValue{
		":prev": &dynamodb.AttributeValue{
			B: expected.Value,
		},
	})
	_, err = b.svc.PutItemWithContext(ctx, &input)
	err = convertError(err)
	if err != nil {
		// in this case let's use more specific compare failed error
		if trace.IsAlreadyExists(err) {
			return nil, trace.CompareFailed(err.Error())
		}
		return nil, trace.Wrap(err)
	}
	return b.newLease(replaceWith), nil
}

// Delete deletes item by key
func (b *DynamoDBBackend) Delete(ctx context.Context, key []byte) error {
	if len(key) == 0 {
		return trace.BadParameter("missing parameter key")
	}
	if _, err := b.getKey(ctx, key); err != nil {
		return err
	}
	return b.deleteKey(ctx, key)
}

// NewWatcher returns a new event watcher
func (b *DynamoDBBackend) NewWatcher(ctx context.Context, watch backend.Watch) (backend.Watcher, error) {
	select {
	case <-b.watchStarted.Done():
	case <-ctx.Done():
		return nil, trace.ConnectionProblem(ctx.Err(), "context is closing")
	}
	return b.buf.NewWatcher(ctx, watch)
}

// KeepAlive keeps object from expiring, updates lease on the existing object,
// expires contains the new expiry to set on the lease,
// some backends may ignore expires based on the implementation
// in case if the lease managed server side
func (b *DynamoDBBackend) KeepAlive(ctx context.Context, lease backend.Lease, expires time.Time) error {
	if len(lease.Key) == 0 {
		return trace.BadParameter("lease is missing key")
	}
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":expires": {
				N: aws.String(strconv.FormatInt(expires.UTC().Unix(), 10)),
			},
		},
		TableName: aws.String(b.Tablename),
		Key: map[string]*dynamodb.AttributeValue{
			hashKeyKey: {
				S: aws.String(hashKey),
			},
			fullPathKey: {
				S: aws.String(string(lease.Key)),
			},
		},
		UpdateExpression: aws.String("SET Expires = :expires"),
	}
	_, err := b.svc.UpdateItemWithContext(ctx, input)
	return convertError(err)
}

// Close closes the DynamoDB driver
// and releases associated resources
func (b *DynamoDBBackend) Close() error {
	b.cancel()
	return b.buf.Close()
}

type tableStatus int

const (
	tableStatusError = iota
	tableStatusMissing
	tableStatusNeedsMigration
	tableStatusOK
)

// Clock returns wall clock
func (b *DynamoDBBackend) Clock() clockwork.Clock {
	return b.clock
}

func (b *DynamoDBBackend) newLease(item backend.Item) *backend.Lease {
	var lease backend.Lease
	if item.Expires.IsZero() {
		return &lease
	}
	lease.Key = item.Key
	return &lease
}

// getTableStatus checks if a given table exists
func (b *DynamoDBBackend) getTableStatus(ctx context.Context, tableName string) (tableStatus, error) {
	td, err := b.svc.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	err = convertError(err)
	if err != nil {
		if trace.IsNotFound(err) {
			return tableStatusMissing, nil
		}
		return tableStatusError, trace.Wrap(err)
	}
	for _, attr := range td.Table.AttributeDefinitions {
		if *attr.AttributeName == oldPathAttr {
			return tableStatusNeedsMigration, nil
		}
	}
	return tableStatusOK, nil
}

// createTable creates a DynamoDB table with a requested name and applies
// the back-end schema to it. The table must not exist.
//
// rangeKey is the name of the 'range key' the schema requires.
// currently is always set to "FullPath" (used to be something else, that's
// why it's a parameter for migration purposes)
func (b *DynamoDBBackend) createTable(ctx context.Context, tableName string, rangeKey string) error {
	pThroughput := dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(b.ReadCapacityUnits),
		WriteCapacityUnits: aws.Int64(b.WriteCapacityUnits),
	}
	def := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(hashKeyKey),
			AttributeType: aws.String("S"),
		},
		{
			AttributeName: aws.String(rangeKey),
			AttributeType: aws.String("S"),
		},
	}
	elems := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(hashKeyKey),
			KeyType:       aws.String("HASH"),
		},
		{
			AttributeName: aws.String(rangeKey),
			KeyType:       aws.String("RANGE"),
		},
	}
	c := dynamodb.CreateTableInput{
		TableName:             aws.String(tableName),
		AttributeDefinitions:  def,
		KeySchema:             elems,
		ProvisionedThroughput: &pThroughput,
	}
	_, err := b.svc.CreateTable(&c)
	if err != nil {
		return trace.Wrap(err)
	}
	b.Infof("Waiting until table %q is created.", tableName)
	err = b.svc.WaitUntilTableExistsWithContext(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		b.Infof("Table %q has been created.", tableName)
	}
	return trace.Wrap(err)
}

// deleteTable deletes DynamoDB table with a given name
func (b *DynamoDBBackend) deleteTable(ctx context.Context, tableName string, wait bool) error {
	tn := aws.String(tableName)
	_, err := b.svc.DeleteTable(&dynamodb.DeleteTableInput{TableName: tn})
	if err != nil {
		return trace.Wrap(err)
	}
	if wait {
		return trace.Wrap(
			b.svc.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{TableName: tn}))
	}
	return nil
}

type getResult struct {
	records []record
}

// getRecords retrieves all keys by path
func (b *DynamoDBBackend) getRecords(ctx context.Context, startKey, endKey string, limit int) (*getResult, error) {
	query := "HashKey = :hashKey AND FullPath BETWEEN :fullPath AND :rangeEnd"
	attrV := map[string]interface{}{
		":fullPath":  startKey,
		":hashKey":   hashKey,
		":timestamp": b.clock.Now().UTC().Unix(),
		":rangeEnd":  endKey,
	}

	// filter out expired items, otherwise they might show up in the query
	// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html
	filter := fmt.Sprintf("attribute_not_exists(Expires) OR Expires >= :timestamp")
	av, err := dynamodbattribute.MarshalMap(attrV)
	if err != nil {
		return nil, convertError(err)
	}
	input := dynamodb.QueryInput{
		KeyConditionExpression:    aws.String(query),
		TableName:                 &b.Tablename,
		ExpressionAttributeValues: av,
		FilterExpression:          aws.String(filter),
		ConsistentRead:            aws.Bool(true),
	}
	if limit > 0 {
		input.Limit = aws.Int64(int64(limit))
	}
	out, err := b.svc.Query(&input)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var result getResult
	for _, item := range out.Items {
		var r record
		dynamodbattribute.UnmarshalMap(item, &r)
		result.records = append(result.records, r)
	}
	sort.Sort(records(result.records))
	result.records = removeDuplicates(result.records)
	return &result, nil
}

// isExpired returns 'true' if the given object (record) has a TTL and
// it's due.
func (r *record) isExpired() bool {
	if r.Expires == nil {
		return false
	}
	expiryDateUTC := time.Unix(*r.Expires, 0).UTC()
	return time.Now().UTC().After(expiryDateUTC)
}

func removeDuplicates(elements []record) []record {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []record{}

	for v := range elements {
		if encountered[elements[v].FullPath] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			encountered[elements[v].FullPath] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}

const (
	modeCreate = iota
	modePut
	modeUpdate
)

// create helper creates a new key/value pair in Dynamo with a given expiration
// depending on mode, either creates, updates or forces create/update
func (b *DynamoDBBackend) create(ctx context.Context, item backend.Item, mode int) error {
	r := record{
		HashKey:   hashKey,
		FullPath:  string(item.Key),
		Value:     item.Value,
		Timestamp: time.Now().UTC().Unix(),
		ID:        time.Now().UTC().UnixNano(),
	}
	if !item.Expires.IsZero() {
		r.Expires = aws.Int64(item.Expires.UTC().Unix())
	}
	av, err := dynamodbattribute.MarshalMap(r)
	if err != nil {
		return trace.Wrap(err)
	}
	input := dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(b.Tablename),
	}
	switch mode {
	case modeCreate:
		input.SetConditionExpression("attribute_not_exists(FullPath)")
	case modeUpdate:
		input.SetConditionExpression("attribute_exists(FullPath)")
	case modePut:
	default:
		return trace.BadParameter("unrecognized mode")
	}
	_, err = b.svc.PutItemWithContext(ctx, &input)
	err = convertError(err)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (b *DynamoDBBackend) deleteKey(ctx context.Context, key []byte) error {
	av, err := dynamodbattribute.MarshalMap(keyLookup{
		HashKey:  hashKey,
		FullPath: string(key),
	})
	if err != nil {
		return trace.Wrap(err)
	}
	input := dynamodb.DeleteItemInput{Key: av, TableName: aws.String(b.Tablename)}
	if _, err = b.svc.DeleteItemWithContext(ctx, &input); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (b *DynamoDBBackend) getKey(ctx context.Context, key []byte) (*record, error) {
	av, err := dynamodbattribute.MarshalMap(keyLookup{
		HashKey:  hashKey,
		FullPath: string(key),
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	input := dynamodb.GetItemInput{
		Key:            av,
		TableName:      aws.String(b.Tablename),
		ConsistentRead: aws.Bool(true),
	}
	out, err := b.svc.GetItem(&input)
	if err != nil || len(out.Item) == 0 {
		return nil, trace.NotFound("%q is not found", string(key))
	}
	var r record
	dynamodbattribute.UnmarshalMap(out.Item, &r)
	// Check if key expired, if expired delete it
	if r.isExpired() {
		b.deleteKey(ctx, key)
		return nil, trace.NotFound("%v is not found", string(key))
	}
	return &r, nil
}

func convertError(err error) error {
	if err == nil {
		return nil
	}
	aerr, ok := err.(awserr.Error)
	if !ok {
		return err
	}
	switch aerr.Code() {
	case dynamodb.ErrCodeConditionalCheckFailedException:
		return trace.CompareFailed(aerr.Error())
	case dynamodb.ErrCodeProvisionedThroughputExceededException:
		return trace.ConnectionProblem(aerr, aerr.Error())
	case dynamodb.ErrCodeResourceNotFoundException:
		return trace.NotFound(aerr.Error())
	case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
		return trace.BadParameter(aerr.Error())
	case dynamodb.ErrCodeInternalServerError:
		return trace.BadParameter(aerr.Error())
	case dynamodbstreams.ErrCodeExpiredIteratorException, dynamodbstreams.ErrCodeLimitExceededException, dynamodbstreams.ErrCodeTrimmedDataAccessException:
		return trace.ConnectionProblem(aerr, aerr.Error())
	default:
		return err
	}
}

type records []record

// Len is part of sort.Interface.
func (r records) Len() int {
	return len(r)
}

// Swap is part of sort.Interface.
func (r records) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// Less is part of sort.Interface.
func (r records) Less(i, j int) bool {
	return r[i].FullPath < r[j].FullPath
}
