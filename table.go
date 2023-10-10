package ddsspfile

import (
	"encoding/json"
	"fmt"
	"github.com/892294101/dds/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"regexp"
)

type TableList struct {
	tableList       map[ownerTable]*ETL           // 参数提取的数据
	schemaTableList map[string]map[string]*string // 过滤基数据
	include         map[string]map[string]bool    // 过滤的数据，包含或排除
}

type TableSets struct {
	supportParams  map[string]map[string]string
	paramPrefix    *string
	TableBus       *TableList
	tableListIndex []ownerTable
}

func (d *TableList) filterInclude(owner, table *string, log *logrus.Logger) bool {
	// 加载参数文件中的库和表到 schemaTableList
	for st, _ := range d.tableList {
		v, ok := d.schemaTableList[st.ownerValue]
		if ok {
			v[st.tableValue] = nil
		} else {
			d.schemaTableList[st.ownerValue] = map[string]*string{st.tableValue: nil}
		}
	}

	v, ok := d.include[*owner][*table]
	if ok {
		return v
	} else {
		stOk := d.filterTableList(owner, table, log)
		if stOk {
			d.include[*owner] = map[string]bool{*table: true}
			log.Infof("add schema and table to table filter whitelist(schema: %v. table: %v)", *owner, *table)
		} else {
			d.include[*owner] = map[string]bool{*table: false}
			log.Infof("add schema and table to table filter blacklist(schema: %v. table: %v)", *owner, *table)
		}
		return stOk
	}
}

func (d *TableList) filterTableList(owner, table *string, log *logrus.Logger) bool {
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
func (d *TableList) Filter(owner, table *string, log *logrus.Logger) (bool, error) {
	if owner == nil {
		return false, errors.Errorf("filter owner name cannot be null")
	}
	if table == nil {
		return false, errors.Errorf("filter table name cannot be null")
	}
	if log == nil {
		return false, errors.Errorf("filter Logger be null")
	}
	return d.filterInclude(owner, table, log), nil
}

func (t *TableSets) put() string {
	var msg string
	for i, index := range t.tableListIndex {
		_, ok := t.TableBus.tableList[index]
		if ok {
			if i > 0 {
				msg += fmt.Sprintf("\n")
			}
			msg += fmt.Sprintf("%s %s.%s;", *t.paramPrefix, index.ownerValue, index.tableValue)
		}

	}
	msg += fmt.Sprintf("\n")
	return msg
}

// 当传入参数时, 初始化特定参数的值
func (t *TableSets) init() {
	t.supportParams = map[string]map[string]string{
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
func (t *TableSets) initDefault() error {
	t.init()
	t.paramPrefix = &utils.DBOptionsType
	return nil
}

func (t *TableSets) isType(raw *string, dbType *string, processType *string) error {
	t.init()
	_, ok := t.supportParams[*dbType][*processType]
	if ok {
		return nil
	}
	return errors.Errorf("The %s %s process does not support this parameter: %s", *dbType, *processType, *raw)
}

// 新参数进入后, 第一次需要进入解析动作
func (t *TableSets) parse(raw *string) error {
	reg, err := regexp.Compile(utils.TableRegular)
	if reg == nil || err != nil {
		return errors.Errorf("%s parameter Regular compilation error: %s", utils.TableType, *raw)
	}

	result := reg.FindStringSubmatch(*raw)
	if len(result) < 1 {
		return errors.Errorf("%s parameter Regular get substring error: %s", utils.TableType, *raw)
	}
	result = utils.TrimKeySpace(result)

	if t.paramPrefix == nil {
		t.paramPrefix = &result[1]
	}

	ownerTable := ownerTable{ownerValue: ValToUper(result[3]), tableValue: ValToUper(result[5])}
	_, ok := t.TableBus.tableList[ownerTable]
	if !ok {
		t.TableBus.tableList[ownerTable] = nil
		t.tableListIndex = append(t.tableListIndex, ownerTable)
	}

	return nil
	/*matched, _ := regexp.MatchString(utils.TableRegular, *raw)
	if matched == true {
		rawText := *raw
		rawText = rawText[:len(rawText)-1]

		tab := utils.TrimKeySpace(strings.Split(rawText, " "))
		for i := 0; i < len(tab); i++ {
			if strings.EqualFold(tab[i], utils.TableType) {
				t.ParamPrefix = &tab[i]
			} else {
				tabVal := strings.Split(tab[i], ".")
				ownerTable := ownerTable{tabVal[0], tabVal[1]}
				_, ok := t.TableBus[ownerTable]
				if !ok {
					t.TableBus[ownerTable] = nil
					t.tableListIndex = append(t.tableListIndex, ownerTable)
				}

			}
		}
		return nil
	}

	if ok := strings.HasSuffix(*raw, ";"); !ok {
		return errors.Errorf("%s parameter must end with a semicolon: %s", utils.TableType, *raw)
	}

	return errors.Errorf("Incorrect %s parameter user(or db) and table Name rules: %s", utils.TableType, *raw)*/
}

// 当出现第二次参数进入, 需要进入add动作
func (t *TableSets) add(raw *string) error {
	reg, err := regexp.Compile(utils.TableRegular)
	if reg == nil || err != nil {
		return errors.Errorf("%s parameter Regular compilation error: %s", utils.TableType, *raw)
	}

	result := reg.FindStringSubmatch(*raw)
	if len(result) < 1 {
		return errors.Errorf("%s parameter Regular get substring error: %s", utils.TableType, *raw)
	}
	result = utils.TrimKeySpace(result)

	ownerTable := ownerTable{ownerValue: ValToUper(result[3]), tableValue: ValToUper(result[5])}
	_, ok := t.TableBus.tableList[ownerTable]
	if !ok {
		t.TableBus.tableList[ownerTable] = nil
		t.tableListIndex = append(t.tableListIndex, ownerTable)
	}

	/*matched, _ := regexp.MatchString(utils.TableRegular, *raw)
	if matched == true {
		rawText := *raw
		rawText = rawText[:len(rawText)-1]

		tab := utils.TrimKeySpace(strings.Split(rawText, " "))
		for i := 0; i < len(tab); i++ {
			if strings.EqualFold(tab[i], utils.TableType) {
				t.ParamPrefix = &tab[i]
			} else {
				tabVal := strings.Split(tab[i], ".")
				ownerTable := ownerTable{tabVal[0], tabVal[1]}
				_, ok := t.TableBus[ownerTable]
				if !ok {
					t.TableBus[ownerTable] = nil
					t.tableListIndex = append(t.tableListIndex, ownerTable)
				}

			}
		}
		return nil
	}
	if ok := strings.HasSuffix(*raw, ";"); !ok {
		return errors.Errorf("%s parameter must end with a semicolon: %s", utils.TableType, *raw)
	}
	return errors.Errorf("Incorrect %s parameter user(or db) and table Name rules: %s", utils.TableType, *raw)
	*/
	return nil
}

type TableSet struct {
	table *TableSets
}

func (t *TableSet) MarshalJson() ([]byte, error) {
	var tjSet []TableJson
	for table := range t.table.TableBus.tableList {
		var tj TableJson
		tj.Type = t.table.paramPrefix
		tj.Owner = table.ownerValue
		tj.Table = table.tableValue
		tjSet = append(tjSet, tj)
	}
	tj, err := json.Marshal(tjSet)
	return tj, err
}

var TableSetBus TableSet

func (t *TableSet) Init() {
	t.table = new(TableSets)
	t.table.TableBus = new(TableList)
	t.table.TableBus.tableList = make(map[ownerTable]*ETL)
	t.table.TableBus.schemaTableList = make(map[string]map[string]*string)
	t.table.TableBus.include = make(map[string]map[string]bool)
}

func (t *TableSet) Add(raw *string) error {
	return t.table.add(raw)
}

func (t *TableSet) ListParamText() string {
	return t.table.put()
}

func (t *TableSet) GetParam() interface{} {
	return t.table
}

func (t *TableSet) Registry() map[string]Parameter {
	t.Init()
	return map[string]Parameter{utils.TableType: t.table}
}
