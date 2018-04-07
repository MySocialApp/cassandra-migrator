package main

import (
	"github.com/spf13/cobra"
	"fmt"
)

var FromHost = ""
var ToHost = ""
var FromKeyspace = ""
var ToKeyspace = ""
var Table = ""

var transferCmd = &cobra.Command{
	Use:   "transfer [COMMANDS]",
	Short: "migrate data from one cassandra instance to another one",
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
		c := Cassandra{}
		c.TransferCassandraData(FromHost, ToHost, FromKeyspace, ToKeyspace, Table)
	},
}

func init() {
	transferCmd.Flags().StringVarP(&FromHost, "from-host", "f", FromHost, "cassandra1:9042")
	transferCmd.Flags().StringVarP(&FromKeyspace, "from-keyspace", "i", FromKeyspace, "old_keyspace_name")
	transferCmd.Flags().StringVarP(&ToHost, "to-host", "t", ToHost, "cassandra2:9042")
	transferCmd.Flags().StringVarP(&ToKeyspace, "to-keyspace", "o", ToKeyspace, "new_keyspace_name")
	transferCmd.Flags().StringVarP(&Table, "table", "a", ToKeyspace, "table_to_sync")

	rootCmd.AddCommand(transferCmd)
}
