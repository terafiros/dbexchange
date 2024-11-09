package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"log"
	"os"
	"strings"
)

/*
func SendData(config) {

}
*/

type Parent struct {
	Configuration []Config `json:"config"`
}

type Config struct {
	Name              string        `json:"name"`
	DependsOn         string        `json:"depends_on"`
	SourceDatabaseUrl string        `json:"source_database_url"`
	TargetDatabaseUrl string        `json:"target_database_url"`
	TablesConfig      []TableConfig `json:"tables_config"`
}

type TableConfig struct {
	SourceName    string         `json:"source_name"`
	SourceFields  string         `json:"source_fields"`
	SourceFilters []SourceFilter `json:"source_filters"`
	TargetName    string         `json:"target_name"`
}

type SourceFilter struct {
	Field     string `json:"field"`
	Value     string `json:"value"`
	Operation string `json:"operation"`
}

type Operation struct {
	Symbol string `json:"operation"`
}

func (operation *Operation) String() string {
	return operation.Symbol
}

func OperationsRegistry() map[string]Operation {
	equal := Operation{"="}
	result := make(map[string]Operation)
	result[equal.Symbol] = equal

	return result
}

var Operations = OperationsRegistry()

func GetConfigs() *Parent {
	content, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatal("Unable to read config file.", err)
	}

	var config Parent
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Unable to parse config file.", err)
	}
	return &config
}

func getDatabaseConn(config Config) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), config.SourceDatabaseUrl)
	if err != nil {
		log.Fatal("Unable to connect to source database.", err)
	}
	return conn
}

func convertToSting(data []any) []string {
	result := make([]string, len(data))
	for _, d := range data {
		result = append(result, fmt.Sprintf("%v", d))
	}
	return result
}

func main() {
	configs := GetConfigs()
	fmt.Println(configs)

	for _, config := range configs.Configuration {
		sourceConn := getDatabaseConn(config)
		defer sourceConn.Close(context.Background())

		targetConn := getDatabaseConn(config)
		defer targetConn.Close(context.Background())

		var selectQuery string
		var insertQuery string

		log.Println("Initializing the", config.Name, "process")
		for _, table := range config.TablesConfig {
			selectQuery = fmt.Sprintf("SELECT %s FROM %s \n", table.SourceFields, table.SourceName)
			insertQuery = fmt.Sprintf("INSERT INTO %s (%s) \n", table.TargetName, table.SourceFields)
			if len(table.SourceFilters) > 0 {
				selectQuery += "WHERE \n"
				for _, filter := range table.SourceFilters {
					selectQuery += fmt.Sprintf("%s %s %s \n", filter.Field, filter.Operation, filter.Value)
				}
			}
			log.Println(selectQuery)
		}
		if len(selectQuery) == 0 {
			log.Fatal("Invalid query")
		}

		rows, err := sourceConn.Query(context.Background(), selectQuery)
		chunkSize := 1000
		insertQuery += "VALUES \n"
		if err != nil {
			log.Fatal("Unable to get data from", config.Name, err)
		}
		count := 0
		for rows.Next() {
			data, err := rows.Values()
			if err != nil {
				log.Println("Unable to get rows to", config.Name, err)
			}
			dataConverted := convertToSting(data)
			insertQuery += "("
			insertQuery += strings.Join(dataConverted, ",")
			insertQuery += ")"

			count += 1

			if count >= chunkSize {
				_, err = targetConn.Exec(context.Background(), insertQuery)
				if err != nil {
					log.Fatal("Unable to insert data into", config.Name, err)
				}
			}

		}
		_, err = targetConn.Exec(context.Background(), insertQuery)
		if err != nil {
			log.Fatal("Unable to insert data into", config.Name, err)
		}
		log.Println("Finish insert to", config.Name)

	}
}
