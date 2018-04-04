package commands

import (
	"github.com/spf13/cobra"
	"log"
	"fmt"
)

var rootCmd = &cobra.Command{
	Use: "cassandra-migrator [COMMANDS]",
	Long: `
[Cassandra I]---/migrate data/-->[Cassandra II]

Fed up with native COPY command? That's why this tool exists. 
It streams data from cassandra instance to another instance (or the same one). This tool was made for our purpose at MySocialApp, 
the need to migrate data from one cluster to another one.`,
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of cassandra-migrator",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("v0.1")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.AddCommand(versionCmd)
}

func initConfig() {
	// initial config
}

func main() {
	rootCmd.Execute()
}
