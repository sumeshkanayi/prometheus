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
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/discovery/refresh"
	"github.com/prometheus/prometheus/discovery/targetgroup"

	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type target struct {
	host string
	port string
	path string
}

const (
	hostname = model.MetaLabelPrefix + "hostname_"
)

// DefaultSDConfig is the default EC2 SD configuration.
var DefaultSDConfig = SDConfig{
	RefreshInterval: model.Duration(10 * time.Second),
	DBSsl:          "disable",
	DBType:         "postgres",
	DBHost:         "localhost",
	DBPort:         "5432",
	
}

// Filter is the configuration for filtering EC2 instances.

// SDConfig is the configuration for EC2 based service discovery.
type SDConfig struct {
	RefreshInterval model.Duration `yaml:"refresh_interval,omitempty"`
	DBHost         string         `yaml:"db_host"`
	DBPassword     string         `yaml:"db_password"`
	DBUser      string `yaml:"db_user"`
	DBName      string `yaml:"db_name"`
	DBSsl       string `yaml:"db_ssl"`
	DBType      string `yaml:"db_type"`
	DBPort      string `yaml:"db_port"`
	MetricsPath string `yaml:"metrics_path"`
	ShardID string `yaml:"shard_id"`

	
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *SDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultSDConfig
	type plain SDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}

	return nil
}

// Discovery periodically performs EC2-SD requests. It implements
// the Discoverer interface.
type Discovery struct {
	*refresh.Discovery
	DBHost      string
	DBPassword  string
	DBUser      string
	DBName      string
	DBSsl       string
	DBType      string
	MetricsPath string
	DBPort      string
	ShardID string
	logger       log.Logger

}

// NewDiscovery returns a new Database discovery which periodically refreshes its targets.
func NewDiscovery(conf *SDConfig, logger log.Logger) *Discovery {

	level.Error(logger).Log("msg", "Initiating database discovery,database host", conf.DBHost, conf.DBName)

	fmt.Println("Initiating Database discovery")

	d := &Discovery{

		DBHost:      conf.DBHost,
		DBPassword:  conf.DBPassword,
		DBUser:      conf.DBUser,
		DBName:      conf.DBName,
		DBSsl:       conf.DBSsl,
		DBType:      conf.DBType,
		MetricsPath: conf.MetricsPath,
		logger:       logger,
		DBPort:      conf.DBPort,
		ShardID: conf.ShardID,
	}
	d.Discovery = refresh.NewDiscovery(
		logger,
		"database",
		time.Duration(conf.RefreshInterval),
		d.refresh,
	)

	return d
}

func (d *Discovery) refresh(ctx context.Context) ([]*targetgroup.Group, error) {

	targGroups := query_db(*d)

	tg := &targetgroup.Group{
		Source: "database",
	}

	for _, val := range targGroups {

		labels := model.LabelSet{

			hostname: model.LabelValue(val.host),
		}
		labels[model.AddressLabel] = model.LabelValue(val.host + ":" + val.port)
		if val.path == "" {
			labels[model.MetricsPathLabel] = model.LabelValue(d.MetricsPath)
		} else {
			labels[model.MetricsPathLabel] = model.LabelValue(val.path)
		}

		tg.Targets = append(tg.Targets, labels)

	}

	return []*targetgroup.Group{tg}, nil
}

func query_db(d Discovery) []target {
	var targetGroup []target
	level.Info(d.logger).Log("msg", "Querying database", "DbHost", d.DBHost, "DB", d.DBName, "DRIVER", d.DBType, "SSL", d.DBSsl)

	//fmt.Printf("Query came for %s and %s",d.DbHost)
	//connStr := "user=postgres dbname=postgres password=password  sslmode=disable host=192.168.1.2"
	connStr:=fmt.Sprintf("user=%s dbname=%s password=%s sslmode=%s host=%s port=%s",d.DBUser,d.DBName,d.DBPassword,d.DBSsl,d.DBHost,d.DBPort)

	//connStr := "user=" + d.DbUser + " dbname=" + d.DBName + " password=" + d.DbPassword + "  sslmode=" + d.Dbssl + " host=" + d.DbHost + " port=" + d.DBPort
	//fmt.Printf("Connection string is %s",connStr)

	db, _ := sql.Open(d.DBType, connStr)
	defer db.Close()
	db_err := db.Ping()

	if db_err == nil {

		rows, err := db.Query("SELECT host,port,path FROM public.prometheus where shard_id = ?",d.ShardID)

		var targets target

		if err == nil {

			defer rows.Close()

			for rows.Next() {

				if err := rows.Scan(&targets.host, &targets.port, &targets.path); err != nil {
					// Check for a scan error.
					// Query rows will be closed with defer.

				}
				targetGroup = append(targetGroup, targets)

			}

		}

	} else {

		level.Error(d.logger).Log("msg", db_err, "DBHost", d.DBHost, "Db", d.DBName)

	}
	return targetGroup
}
