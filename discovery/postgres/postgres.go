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
	Db_ssl:          "disable",
	DB_type:         "postgres",
	Db_host:         "localhost",
	DB_port:         "5432",
	
}

// Filter is the configuration for filtering EC2 instances.

// SDConfig is the configuration for EC2 based service discovery.
type SDConfig struct {
	RefreshInterval model.Duration `yaml:"refresh_interval,omitempty"`
	Db_host         string         `yaml:"db_host"`
	Db_password     string         `yaml:"db_password"`

	Db_user      string `yaml:"db_user"`
	Db_name      string `yaml:"db_name"`
	Db_ssl       string `yaml:"db_ssl"`
	DB_type      string `yaml:"db_type"`
	DB_port      string `yaml:"db_port"`
	Metrics_path string `yaml:"metrics_path"`
	logger       log.Logger
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
	db_host      string
	db_password  string
	db_user      string
	db_name      string
	db_ssl       string
	db_type      string
	metrics_path string
	db_port      string
	logger       log.Logger
}

// NewDiscovery returns a new Database discovery which periodically refreshes its targets.
func NewDiscovery(conf *SDConfig, logger log.Logger) *Discovery {

	level.Error(logger).Log("msg", "Initiating database discovery,database host", conf.Db_host, conf.Db_name)

	fmt.Println("Initiating Database discovery")

	d := &Discovery{

		db_host:      conf.Db_host,
		db_password:  conf.Db_password,
		db_user:      conf.Db_user,
		db_name:      conf.Db_name,
		db_ssl:       conf.Db_ssl,
		db_type:      conf.DB_type,
		metrics_path: conf.Metrics_path,
		logger:       logger,
		db_port:      conf.DB_port,
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
			labels[model.MetricsPathLabel] = model.LabelValue(d.metrics_path)
		} else {
			labels[model.MetricsPathLabel] = model.LabelValue(val.path)
		}

		tg.Targets = append(tg.Targets, labels)

	}

	return []*targetgroup.Group{tg}, nil
}

func query_db(d Discovery) []target {
	var targetGroup []target
	level.Info(d.logger).Log("msg", "Querying database", "DB_HOST", d.db_host, "DB", d.db_name, "DRIVER", d.db_type, "SSL", d.db_ssl)

	//fmt.Printf("Query came for %s and %s",d.db_host)
	//connStr := "user=postgres dbname=postgres password=password  sslmode=disable host=192.168.1.2"

	connStr := "user=" + d.db_user + " dbname=" + d.db_name + " password=" + d.db_password + "  sslmode=" + d.db_ssl + " host=" + d.db_host + " port=" + d.db_port
	//fmt.Printf("Connection string is %s",connStr)

	db, _ := sql.Open(d.db_type, connStr)
	defer db.Close()
	db_err := db.Ping()

	if db_err == nil {

		rows, err := db.Query("SELECT host,port,path FROM public.prometheus")

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

		level.Error(d.logger).Log("msg", db_err, "DB_HOST", d.db_host, "Db", d.db_name)

	}
	return targetGroup
}
