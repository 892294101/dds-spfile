package dds_spfile

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

type configServer struct {
	path   string
	handle *sql.DB
	log    *logrus.Logger
}

func (c *configServer) init(path string, log *logrus.Logger) {
	c.path = path
	c.log = log
}

func (c *configServer) open() error {
	db, err := sql.Open("sqlite3", c.path)
	if err != nil {
		return err
	}
	c.handle = db
	return nil
}

func (c *configServer) LoadJsonToServerConfig(data []string, processName *string) error {
	c.handle.Exec(fmt.Sprintf("DROP TABLE %s", *processName))

	_, err := c.handle.Exec(fmt.Sprintf("CREATE TABLE %s (RULE STRING(4000))", *processName))
	if err != nil {
		return err
	}

	res, err := c.handle.Prepare(fmt.Sprintf("INSERT INTO %s (RULE) VALUES(?)", *processName))
	if err != nil {
		return err
	}
	for _, valSet := range data {
		_, err := res.Exec(valSet)
		if err != nil {
			return err
		}
	}

	if err := res.Close(); err != nil {
		return err
	}
	return c.handle.Close()
}
