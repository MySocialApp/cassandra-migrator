package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Cassandra struct {
}

func (c *Cassandra) getCassandraSession(host string) *gocql.Session {
	mHost := host
	mPort := 9042
	if strings.Contains(host, ":") {
		values := strings.Split(host, ":")
		mHost = values[0]

		p, err := strconv.Atoi(values[1])
		if err != nil {
			panic(err)
		}

		mPort = p
	}

	clusterConfig := gocql.NewCluster(mHost)
	clusterConfig.Port = mPort
	clusterConfig.Timeout = 60 * time.Second
	clusterConfig.Consistency = gocql.Quorum

	session, err := clusterConfig.CreateSession()

	if err != nil {
		panic(fmt.Sprintf("Error connecting to Cassandra %s", err.Error()))
	}

	return session
}

func (c *Cassandra) getCreateTableQuery(keyspace string, table *gocql.TableMetadata) string {
	var columns []string
	var orderedColumns []string

	for col, data := range table.Columns {
		columns = append(columns, col+" "+data.Validator)

		if data.Order == true {
			orderedColumns = append(orderedColumns, col+" "+data.ClusteringOrder)
		}
	}

	var pkColumns []string
	for _, pk := range table.PartitionKey {
		pkColumns = append(pkColumns, pk.Name)
	}

	var clusteringColumns []string
	for _, column := range table.ClusteringColumns {
		clusteringColumns = append(clusteringColumns, column.Name)
	}

	if len(orderedColumns) > 0 && len(clusteringColumns) > 0 {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s), %s)) WITH CLUSTERING ORDER BY (%s);",
			keyspace, table.Name, strings.Join(columns, ","), strings.Join(pkColumns, ","),
			strings.Join(clusteringColumns, ","), strings.Join(orderedColumns, ","))
	}

	if len(clusteringColumns) > 0 {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s), %s));",
			keyspace, table.Name, strings.Join(columns, ","), strings.Join(pkColumns, ","),
			strings.Join(clusteringColumns, ","))
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s)));",
		keyspace, table.Name, strings.Join(columns, ","), strings.Join(pkColumns, ","))
}

func (c *Cassandra) getTableColumnsName(results map[string]interface{}) []string {
	var columns []string
	for columnName := range results {
		columns = append(columns, columnName)
	}

	return columns
}

func (c *Cassandra) getStringOrNumber(v interface{}) string {
	switch v.(type) {
	case string:
		return fmt.Sprintf("'%v'", strings.Replace(v.(string), "'", "''", -1))
	case time.Time:
		if v.(time.Time).IsZero() {
			return ""
		}

		return "'" + v.(time.Time).Format("2006-01-02 03:04:05") + "'"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (c *Cassandra) getValueString(v interface{}, columnMetadata *gocql.ColumnMetadata) string {
	if columnMetadata.Validator == "text" {
		return c.getStringOrNumber(v)
	}

	switch columnMetadata.Validator {
	case "list<int>", "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)":
		var result []string
		for _, e := range v.([]int) {
			result = append(result, c.getStringOrNumber(e))
		}

		return "[" + strings.Join(result, ",") + "]"
	case "list<bigint>", "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.LongType)":
		var result []string
		for _, e := range v.([]int64) {
			result = append(result, c.getStringOrNumber(e))
		}

		return "[" + strings.Join(result, ",") + "]"
	case "list<text>", "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)":

		var result []string
		for _, e := range v.([]string) {
			result = append(result, c.getStringOrNumber(e))
		}

		return "[" + strings.Join(result, ",") + "]"
	case "set<int>", "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)":
		var result []string
		for _, e := range v.([]int) {
			result = append(result, c.getStringOrNumber(e))
		}

		return "{" + strings.Join(result, ",") + "}"
	case "set<bigint>", "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.LongType)":
		var result []string
		for _, e := range v.([]int64) {
			result = append(result, c.getStringOrNumber(e))
		}

		return "{" + strings.Join(result, ",") + "}"
	case "set<text>", "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)":
		var result []string
		for _, e := range v.([]string) {
			result = append(result, c.getStringOrNumber(e))
		}

		return "{" + strings.Join(result, ",") + "}"
	case "map<text, text>", "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.UTF8Type)":
		var result []string
		for k, v := range v.(map[string]string) {
			result = append(result, c.getStringOrNumber(k)+":"+c.getStringOrNumber(v))
		}

		return "{" + strings.Join(result, ",") + "}"
	case "map<bigint, text>", "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.LongType, org.apache.cassandra.db.marshal.UTF8Type)":
		var result []string
		for k, v := range v.(map[int64]string) {
			result = append(result, c.getStringOrNumber(k)+":"+c.getStringOrNumber(v))
		}

		return "{" + strings.Join(result, ",") + "}"
	default:
		return c.getStringOrNumber(v)
	}
}

func (c *Cassandra) getInsertDataQuery(keyspace string, table *gocql.TableMetadata, results map[string]interface{}) string {
	columnsName := c.getTableColumnsName(results)

	var params []interface{}
	params = append(params, keyspace)
	params = append(params, table.Name)

	removedElementsCount := 0
	columnsNameCopy := make([]string, len(columnsName))
	copy(columnsNameCopy, columnsName)

	var values []string
	for i, columnName := range columnsNameCopy {
		v := c.getValueString(results[columnName], table.Columns[columnName])
		// only append column + value that are not empty
		if v != "" && v != "''" && v != "0" && v != "<nil>" {
			values = append(values, v)
		} else {
			// remove column from columnsName
			idx := i - removedElementsCount
			if len(columnsName) == idx {
				columnsName = columnsName[:idx]
			} else {
				columnsName = append(columnsName[:idx], columnsName[idx+1:]...)
			}

			removedElementsCount++
		}
	}

	if values == nil || len(values) == 0 {
		return ""
	}

	params = append(params, strings.Join(columnsName, ","))
	params = append(params, strings.Join(values, ","))

	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", params...)
}

func (c *Cassandra) createTable(s *gocql.Session, keyspace string, table *gocql.TableMetadata) {
	q := c.getCreateTableQuery(keyspace, table)
	log.Println(q)
	err := s.Query(q).Exec()
	if err != nil {
		panic(err)
	}
}

func (c *Cassandra) syncData(s1 *gocql.Session, s2 *gocql.Session, fromKeyspace string, toKeyspace string,
	skipCreateTables bool, skipRows int, skipInsertRowErrors bool, table *gocql.TableMetadata) {

	log.Println("Sync table data from " + fromKeyspace + "." + table.Name + " to " + toKeyspace + "." + table.Name)
	iter := s1.Query("SELECT * FROM " + fromKeyspace + "." + table.Name).Iter()

	count := 0

	for {
		var row = make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}

		if count >= skipRows {
			// insert data from current table row to S2.table
			q := c.getInsertDataQuery(toKeyspace, table, row)

			if q != "" {
				count++
				err := s2.Query(q).Exec()
				if err != nil && !skipCreateTables {
					log.Println(err)
					log.Println("Query error: " + q)

					if !skipInsertRowErrors {
						panic(err)
					}
				}

				if count%100 == 0 {
					log.Println(toKeyspace + "." + table.Name + ": " + strconv.Itoa(count) + " rows")
				}
			}
		} else {
			count++

			if count%1000 == 0 {
				log.Println(toKeyspace + "." + table.Name + ": " + strconv.Itoa(count) + " skipped rows")
			}
		}
	}

	log.Println(toKeyspace + "." + table.Name + ": " + strconv.Itoa(count) + " rows")
}

func (c *Cassandra) TransferCassandraData(fromHost string, toHost string, fromKeyspace string, toKeyspace string,
	tableToSync string, skipCreateTables bool, skipRows int, skipInsertRowErrors bool) {

	s1 := c.getCassandraSession(fromHost)
	s2 := c.getCassandraSession(toHost)
	// create remote Keyspace
	k, err := s1.KeyspaceMetadata(fromKeyspace)
	if err != nil {
		panic(err)
	}

	err = s2.Query("CREATE KEYSPACE IF NOT EXISTS " + toKeyspace +
		" WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '4tech-fr': 3 };").Exec()

	if err != nil {
		panic(err)
	}

	// create remote Tables
	if !skipCreateTables {
		for _, table := range k.Tables {
			if tableToSync != "" && table.Name == tableToSync {
				c.createTable(s2, toKeyspace, table)
				break
			} else if tableToSync == "" {
				c.createTable(s2, toKeyspace, table)
			}
		}
	}

	log.Println("Tables has been created")
	log.Println("Let's sync " + strconv.Itoa(len(k.Tables)) + " tables data")

	var wg sync.WaitGroup
	wg.Add(len(k.Tables))

	// inject data from S1 to S2
	for _, t := range k.Tables {
		go func(table *gocql.TableMetadata) {
			defer wg.Done()
			if tableToSync != "" && table.Name == tableToSync {
				c.syncData(s1, s2, fromKeyspace, toKeyspace, skipCreateTables, skipRows, skipInsertRowErrors, table)
			} else if tableToSync == "" {
				c.syncData(s1, s2, fromKeyspace, toKeyspace, skipCreateTables, skipRows, skipInsertRowErrors, table)
			}
		}(t)
	}

	wg.Wait()
	log.Println("End of sync")
}
