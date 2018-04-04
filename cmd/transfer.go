package main

import (
	"github.com/spf13/cobra"
	"github.com/gocql/gocql"
	"fmt"
	"strings"
	"strconv"
)

var FromHost = ""
var ToHost = ""
var FromKeyspace = ""
var ToKeyspace = ""

var transferCmd = &cobra.Command{
	Use:   "transfer [COMMANDS]",
	Short: "migrate data from one cassandra instance to another one",
	Args:  cobra.MinimumNArgs(2),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if FromHost == "" {
			return fmt.Errorf("FROM host is mandatory")
		}

		if ToHost == "" {
			return fmt.Errorf("TO host is mandatory")
		}

		if ToKeyspace == "" {
			ToKeyspace = FromKeyspace
		}

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		transferCassandraData(FromHost, ToHost, FromKeyspace, ToKeyspace)
	},
}

func init() {
	transferCmd.Flags().StringVarP(&FromHost, "from-host", "fh", FromHost, "cassandra1:9042")
	transferCmd.Flags().StringVarP(&FromKeyspace, "from-keyspace", "fk", FromKeyspace, "old_keyspace_name")
	transferCmd.Flags().StringVarP(&ToHost, "to-host", "th", ToHost, "cassandra2:9042")
	transferCmd.Flags().StringVarP(&ToKeyspace, "to-keyspace", "tk", ToKeyspace, "new_keyspace_name")

	rootCmd.AddCommand(transferCmd)
}

func getCassandraSession(host string) *gocql.Session {
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
	session, err := clusterConfig.CreateSession()

	if err != nil {
		panic(fmt.Sprintf("Error connecting to Cassandra %s", err.Error()))
	}

	return session
}

func getCreateTableQuery(table *gocql.TableMetadata) string {

}

func getTableColumnsName(results map[string]interface{}) []string {
	var columns []string
	for columnName := range results {
		columns = append(columns, columnName)
	}

	return columns
}

func getInsertDataQuery(table *gocql.TableMetadata, results map[string]interface{}) string {
	columnsName := getTableColumnsName(results)

	var values []interface{}
	values = append(values, table.Keyspace)
	values = append(values, table.Name)

	for _, columnName := range columnsName {
		values = append(values, results[columnName])
	}

	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", values...)
}

func transferCassandraData(fromHost string, toHost string, fromKeyspace string, toKeyspace string) {
	s1 := getCassandraSession(fromHost)
	s2 := getCassandraSession(toHost)

	// TODO create remote Keyspace (name + options)
	s2.Query(`CREATE KEYSPACE IF NOT EXISTS ? WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '4tech-fr': 3 };`, toKeyspace).Consistency(gocql.Quorum).Exec()

	k, _ := s1.KeyspaceMetadata(fromKeyspace)

	// TODO create remote Tables (name + structures)
	for _, table := range k.Tables {
		s2.Query(getCreateTableQuery(table)).Consistency(gocql.Quorum).Exec()
	}

	// TODO inject data from S1 to S2
	m := map[string]interface{}{}
	for _, table := range k.Tables {
		elements := s1.Query(`SELECT * FROM ?.?`, fromKeyspace, k.Name).Consistency(gocql.Quorum).Iter()
		for elements.MapScan(m) {
			// TODO insert data from current table row to S2.table
			s2.Query(getInsertDataQuery(table, m)).Consistency(gocql.Quorum).Exec()
		}

	}
}
