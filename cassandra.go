package main

import (
	"github.com/gocql/gocql"
	"strings"
	"fmt"
	"strconv"
	"log"
	"time"
	"go/types"
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
	case types.Nil:
		return ""
	case string:
		return fmt.Sprintf("'%v'", strings.Replace(v.(string), "'", "''", -1))
	case time.Time:
		return "'" + v.(time.Time).Format("2006-01-02 03:04:05") + "'"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (c *Cassandra) getValueString(v interface{}, columnMetadata *gocql.ColumnMetadata) string {
	switch v.(type) {
	case []interface{}:
		var result []string
		for _, e := range v.([]interface{}) {
			result = append(result, c.getStringOrNumber(e))
		}

		if strings.Contains(columnMetadata.Validator, "set") {
			return "{" + strings.Join(result, ",") + "}"
		}

		return "[" + strings.Join(result, ",") + "]"
	case map[interface{}][]interface{}:
		var result []string
		for k, v := range v.(map[interface{}]interface{}) {
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

	var values []string
	for i, columnName := range columnsName {
		v := c.getValueString(results[columnName], table.Columns[columnName])
		// only append column + value that are not empty
		if v != "" {
			values = append(values, v)
		} else {
			// remove column from columnsName
			columnsName = append(columnsName[:i], columnsName[i+1:]...)
		}
	}

	params = append(params, strings.Join(columnsName, ","))
	params = append(params, strings.Join(values, ","))

	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", params...)
}

func (c *Cassandra) TransferCassandraData(fromHost string, toHost string, fromKeyspace string, toKeyspace string) {
	s1 := c.getCassandraSession(fromHost)
	s2 := c.getCassandraSession(toHost)
	// create remote Keyspace
	err := s2.Query("CREATE KEYSPACE IF NOT EXISTS " + toKeyspace +
		" WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '4tech-fr': 3 };").Consistency(gocql.Quorum).Exec()

	if err != nil {
		panic(err)
	}

	k, _ := s1.KeyspaceMetadata(fromKeyspace)

	// create remote Tables
	for _, table := range k.Tables {
		q := c.getCreateTableQuery(toKeyspace, table)
		log.Println(q)
		err = s2.Query(q).Consistency(gocql.Quorum).Exec()
		if err != nil {
			panic(err)
		}
	}

	log.Println("Tables has been created")
	log.Println("Let's sync tables data")

	// inject data from S1 to S2
	for _, table := range k.Tables {
		log.Println("Sync table data from " + fromKeyspace + "." + table.Name + " to " + toKeyspace + "." + table.Name)
		iter := s1.Query("SELECT * FROM " + fromKeyspace + "." + table.Name).Consistency(gocql.Quorum).Iter()

		count := 1
		for {
			var row = make(map[string]interface{})
			if !iter.MapScan(row) {
				break
			}

			if count%1000 == 0 {
				log.Println(toKeyspace + "." + table.Name + ": " + strconv.Itoa(count) + " rows")
			}
			// insert data from current table row to S2.table
			q := c.getInsertDataQuery(toKeyspace, table, row)
			err := s2.Query(q).Consistency(gocql.Quorum).Exec()
			if err != nil {
				panic(err)
			}

			count++
		}

		log.Println(toKeyspace + "." + table.Name + ": " + strconv.Itoa(count) + " rows")
	}

	log.Println("End of sync")
}
