package ddsspfile

import (
	"encoding/json"
	"fmt"
	"github.com/892294101/dds/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"regexp"
	"strings"
)

type TableExcludeList struct {
	tableList       map[ownerTable]*string        // 参数提取的数据
	schemaTableList map[string]map[string]*string // 过滤基数据
	exclude         map[string]map[string]bool    // 过滤的数据，包含或排除
}

type ExcludeTableSets struct {
	supportParams  map[string]map[string]string
	paramPrefix    *string
	TableBus       *TableExcludeList
	tableListIndex []ownerTable
}

func (d *TableExcludeList) filterExclude(owner, table *string, log *logrus.Logger) bool {
	// 加载参数文件中的库和表到 schemaTableList
	for st, _ := range d.tableList {
		v, ok := d.schemaTableList[st.ownerValue]
		if ok {
			v[st.tableValue] = nil
		} else {
			d.schemaTableList[st.ownerValue] = map[string]*string{st.tableValue: nil}
		}
	}
	if len(d.schemaTableList) > 0 {
		v, ok := d.exclude[*owner][*table]
		if ok {
			return v
		} else {
			stOk := d.filtertableList(owner, table, log)
			if stOk {
				d.exclude[*owner] = map[string]bool{*table: true}
				log.Infof("add schema and table to exclude filter whitelist(schema: %v. table: %v)", *owner, *table)
			} else {
				d.exclude[*owner] = map[string]bool{*table: false}
				log.Infof("add schema and table to exclude filter blacklist(schema: %v. table: %v)", *owner, *table)
			}
			return stOk
		}
	} else {
		return true
	}

}

func (d *TableExcludeList) filtertableList(owner, table *string, log *logrus.Logger) bool {
	_, ok := d.schemaTableList[*owner][*table]
	if ok {
		return true
	} else {
		v, ok := d.schemaTableList[*owner]
		if ok {
			for val, _ := range v {
				if MatchSchemaTable(owner, table, &val, log) {
					return true
				}
			}
		} else {
			return false
		}

	}
	return false
}
func (d *TableExcludeList) Filter(owner, table *string, log *logrus.Logger) (bool, error) {
	if owner == nil {
		return false, errors.Errorf("filter owner name cannot be null")
	}
	if table == nil {
		return false, errors.Errorf("filter table name cannot be null")
	}
	if log == nil {
		return false, errors.Errorf("filter Logger be null")
	}
	return d.filterExclude(owner, table, log), nil
}

func (e *ExcludeTableSets) put() string {
	var msg strings.Builder
	for _, index := range e.tableListIndex {
		_, ok := e.TableBus.tableList[index]
		if ok {
			msg.WriteString(fmt.Sprintf("%s %s.%s;\n", *e.paramPrefix, index.ownerValue, index.tableValue))
		}
	}
	return strings.Trim(msg.String(), "\n")
}

// 当传入参数时, 初始化特定参数的值
func (e *ExcludeTableSets) init() {
	e.supportParams = map[string]map[string]string{
		utils.MySQL: {
			utils.Extract:  utils.Extract,
			utils.Replicat: utils.Replicat,
		},
		utils.Oracle: {
			utils.Extract:  utils.Extract,
			utils.Replicat: utils.Replicat,
		},
	}
}

// 当没有参数时, 初始化此参数默认值
func (e *ExcludeTableSets) initDefault() error {
	e.init()
	e.paramPrefix = &utils.DBOptionsType
	return nil
}

func (e *ExcludeTableSets) isType(raw *string, dbType *string, processType *string) error {
	e.init()
	_, ok := e.supportParams[*dbType][*processType]
	if ok {
		return nil
	}
	return errors.Errorf("The %s %s process does not support this parameter: %s", *dbType, *processType, *raw)
}

// 新参数进入后, 第一次需要进入解析动作
func (e *ExcludeTableSets) parse(raw *string) error {
	reg, err := regexp.Compile(utils.TableExcludeRegular)
	if reg == nil || err != nil {
		return errors.Errorf("%s parameter Regular compilation error: %s", utils.TableExcludeType, *raw)
	}

	result := reg.FindStringSubmatch(*raw)
	if len(result) < 1 {
		return errors.Errorf("%s parameter Regular get substring error: %s", utils.TableExcludeType, *raw)
	}
	result = utils.TrimKeySpace(result)

	if e.paramPrefix == nil {
		e.paramPrefix = &result[1]
	}

	ownerTable := ownerTable{ValToUper(result[3]), ValToUper(result[5])}
	_, ok := e.TableBus.tableList[ownerTable]
	if !ok {
		e.TableBus.tableList[ownerTable] = nil
		e.tableListIndex = append(e.tableListIndex, ownerTable)
	}
	return nil
}

// 当出现第二次参数进入, 需要进入add动作
func (e *ExcludeTableSets) add(raw *string) error {
	reg, err := regexp.Compile(utils.TableExcludeRegular)
	if reg == nil || err != nil {
		return errors.Errorf("%s parameter Regular compilation error: %s", utils.TableExcludeType, *raw)
	}

	result := reg.FindStringSubmatch(*raw)
	if len(result) < 1 {
		return errors.Errorf("%s parameter Regular get substring error: %s", utils.TableExcludeType, *raw)
	}
	result = utils.TrimKeySpace(result)

	ownerTable := ownerTable{ValToUper(result[3]), ValToUper(result[5])}
	_, ok := e.TableBus.tableList[ownerTable]
	if !ok {
		e.TableBus.tableList[ownerTable] = nil
		e.tableListIndex = append(e.tableListIndex, ownerTable)
	}

	return nil
}

type ExcludeTableSet struct {
	table *ExcludeTableSets
}

func (e *ExcludeTableSet) MarshalJson() ([]byte, error) {
	var tjSet []TableExcludeJson
	for table := range e.table.TableBus.tableList {
		var te TableExcludeJson
		te.Type = e.table.paramPrefix
		te.Owner = table.ownerValue
		te.Table = table.tableValue
		tjSet = append(tjSet, te)
	}
	te, err := json.Marshal(tjSet)
	return te, err
}

var ExcludeTableSetBus ExcludeTableSet

func (e *ExcludeTableSet) Init() {
	e.table = new(ExcludeTableSets)
	e.table.TableBus = new(TableExcludeList)
	e.table.TableBus.tableList = make(map[ownerTable]*string)
	e.table.TableBus.schemaTableList = make(map[string]map[string]*string)
	e.table.TableBus.exclude = make(map[string]map[string]bool)
}

func (e *ExcludeTableSet) Add(raw *string) error {
	return e.table.add(raw)
}

func (e *ExcludeTableSet) ListParamText() string {
	return e.table.put()
}

func (e *ExcludeTableSet) GetParam() interface{} {
	return e.table
}

func (e *ExcludeTableSet) Registry() map[string]Parameter {
	e.Init()
	return map[string]Parameter{utils.TableExcludeType: e.table}
}
