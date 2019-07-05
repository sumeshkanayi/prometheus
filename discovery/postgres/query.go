// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	_ "database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	

)

type ConsulSDConfig struct{
Server string `db:"server"`
Services pq.StringArray `db:"services"`
Tags pq.StringArray `db:"tags"`
Datacenter string `db:"datacenter"`
Token string `db:"token"`

}

func Query(class string, shard string) []ConsulSDConfig {

	var SDConfigs []ConsulSDConfig

	connStr := fmt.Sprintf("user=%s dbname=%s password=%s sslmode=%s host=%s port=%s", "devops_admin", "devops_primary", "admin", "disable", "postgres.bdf-cloud.iqvia.net", "5432")
	fmt.Println("connection string is", connStr)

	db, dbErr := sqlx.Connect("postgres", connStr)
	defer db.Close()

	if dbErr == nil {

		dbSelectError := db.Select(&SDConfigs, "SELECT server,services,token,datacenter,tags FROM public.metrics WHERE shardid=$1 AND type=$2", shard, class)
        
		fmt.Println("Target group is", SDConfigs, shard, class, dbSelectError, dbErr)
		if dbSelectError != nil {
			fmt.Println(dbSelectError)
		}

		return SDConfigs

	}

	return SDConfigs

}
