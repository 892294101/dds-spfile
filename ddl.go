package ddsspfile

import (
	"encoding/json"
	"fmt"
	"github.com/892294101/dds-utils"
	"github.com/pkg/errors"
	"strings"
)

type ownTab struct {
	owner string
	table string
}

type ObjectNode struct {
	opType  map[string]string
	objType map[string]string
}

type DdlList struct {
	include []map[ownTab]*ObjectNode // 包含的对象
	exclude []map[ownTab]*ObjectNode // 排除的对象
}

type supportOptionList struct {
	opType  map[string]*string
	objType map[string]*string
}

type supportOptionBus struct {
	opts map[string]*supportOptionList
}

func (d *DdlList) Filter(owner, table, optype, objtype string) bool {
	return false
}

type DdlSmt struct {
	supportParams map[string]map[string]string // 参数支持的数据库和进程
	parseDBType   *string                      // 被解析的数据库类型
	ParamPrefix   *string                      // 参数前缀
	DdlBus        *DdlList                     // DDL列表
	supportOption *supportOptionBus            // 数据库支持的参数选项，例如mysql支持create，oracle支持drop
}

func (d *DdlSmt) put() string {
	var buf strings.Builder
	if d.DdlBus.include != nil {
		for _, m := range d.DdlBus.include {
			for tab, node := range m {
				buf.WriteString(fmt.Sprintf("%s ", strings.ToUpper(*d.ParamPrefix)))
				buf.WriteString(fmt.Sprintf("%s ", ddsutils.INCLUDE))
				buf.WriteString(fmt.Sprintf("%s.%s ", tab.owner, tab.table))

				var ct int

				if len(node.objType) > 0 {
					buf.WriteString("OBJTYPE ")
					for key := range node.objType {
						ct += 1
						if ct == 1 {
							buf.WriteString(key)
						} else {
							buf.WriteString("," + key)
						}

					}
				}

				if len(node.opType) > 0 {
					buf.WriteString(" OPTYPE ")
					ct = 0
					for key := range node.opType {
						ct += 1
						if ct == 1 {
							buf.WriteString(key)
						} else {
							buf.WriteString("," + key)
						}

					}
				}
			}
			buf.WriteString(fmt.Sprintf("\n"))
		}

	}

	if d.DdlBus.exclude != nil {
		for i, m := range d.DdlBus.exclude {
			for tab, node := range m {
				buf.WriteString(fmt.Sprintf("%s ", strings.ToUpper(*d.ParamPrefix)))
				buf.WriteString(fmt.Sprintf("%s ", ddsutils.EXCLUDE))
				buf.WriteString(fmt.Sprintf("%s.%s ", tab.owner, tab.table))
				var ct int
				if len(node.objType) > 0 {
					buf.WriteString("OBJTYPE ")
					for key := range node.objType {
						ct += 1
						if ct == 1 {
							buf.WriteString(key)
						} else {
							buf.WriteString("," + key)
						}

					}
				}

				if len(node.opType) > 0 {
					buf.WriteString(" OPTYPE ")
					ct = 0
					for key := range node.opType {
						ct += 1
						if ct == 1 {
							buf.WriteString(key)
						} else {
							buf.WriteString("," + key)
						}

					}
				}

			}
			if i+1 != len(d.DdlBus.exclude) {
				buf.WriteString(fmt.Sprintf("\n"))
			}

		}
	}
	buf.WriteString(fmt.Sprintf("\n"))
	return buf.String()
}

func (d *DdlSmt) init() {
	// 初始化支持的进程
	d.supportParams = map[string]map[string]string{
		ddsutils.MySQL: {
			ddsutils.Extract:  ddsutils.Extract,
			ddsutils.Replicat: ddsutils.Replicat,
		},
		ddsutils.Oracle: {
			ddsutils.Extract:  ddsutils.Extract,
			ddsutils.Replicat: ddsutils.Replicat,
		},
	}

	// 初始化DDL对象
	d.DdlBus = new(DdlList)
	//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	// 初始化支持对象，针对特定的数据库
	d.supportOption = new(supportOptionBus)
	d.supportOption.opts = make(map[string]*supportOptionList)

	// Oracle 支持的DDL选项
	d.supportOption.opts[ddsutils.Oracle] = new(supportOptionList)
	d.supportOption.opts[ddsutils.Oracle].objType = make(map[string]*string)
	d.supportOption.opts[ddsutils.Oracle].opType = make(map[string]*string)
	// Oracle 支持的操作类型
	d.supportOption.opts[ddsutils.Oracle].opType = map[string]*string{
		ddsutils.CREATE: &ddsutils.CREATE,
		ddsutils.ALTER:  &ddsutils.ALTER,
		ddsutils.DROP:   &ddsutils.DROP,
	}
	// Oracle 支持的操作对象
	d.supportOption.opts[ddsutils.Oracle].objType = map[string]*string{
		ddsutils.TABLE:     &ddsutils.TABLE,
		ddsutils.INDEX:     &ddsutils.INDEX,
		ddsutils.TRIGGER:   &ddsutils.TRIGGER,
		ddsutils.SEQUENCE:  &ddsutils.SEQUENCE,
		ddsutils.VIEW:      &ddsutils.VIEW,
		ddsutils.FUNCTION:  &ddsutils.FUNCTION,
		ddsutils.PACKAGE:   &ddsutils.PACKAGE,
		ddsutils.PROCEDURE: &ddsutils.PROCEDURE,
		ddsutils.TYPE:      &ddsutils.TYPE,
		ddsutils.DATABASE:  &ddsutils.DATABASE,
	}

	// MySQL 支持的DDL选项
	d.supportOption.opts[ddsutils.MySQL] = new(supportOptionList)
	d.supportOption.opts[ddsutils.MySQL].objType = make(map[string]*string)
	d.supportOption.opts[ddsutils.MySQL].opType = make(map[string]*string)
	// Oracle 支持的操作类型
	d.supportOption.opts[ddsutils.MySQL].opType = map[string]*string{
		ddsutils.CREATE: &ddsutils.CREATE,
		ddsutils.ALTER:  &ddsutils.ALTER,
		ddsutils.DROP:   &ddsutils.DROP,
	}
	// Oracle 支持的操作对象
	d.supportOption.opts[ddsutils.MySQL].objType = map[string]*string{
		ddsutils.TABLE:     &ddsutils.TABLE,
		ddsutils.INDEX:     &ddsutils.INDEX,
		ddsutils.TRIGGER:   &ddsutils.TRIGGER,
		ddsutils.SEQUENCE:  &ddsutils.SEQUENCE,
		ddsutils.VIEW:      &ddsutils.VIEW,
		ddsutils.FUNCTION:  &ddsutils.FUNCTION,
		ddsutils.PROCEDURE: &ddsutils.PROCEDURE,
		ddsutils.TYPE:      &ddsutils.TYPE,
		ddsutils.EVENT:     &ddsutils.EVENT,
		ddsutils.USER:      &ddsutils.USER,
		ddsutils.DATABASE:  &ddsutils.DATABASE,
	}

}

func parseDDLText(raw *string, d *DdlSmt) (*string, *ownTab, *ObjectNode, error) {
	uid := ddsutils.TrimKeySpace(strings.Split(*raw, " "))
	uidLength := len(uid) - 1
	var mark string
	var ot ownTab
	var on ObjectNode
	on.opType = make(map[string]string)
	on.objType = make(map[string]string)

	for i := 0; i < len(uid); i++ {
		switch {
		case strings.EqualFold(uid[i], ddsutils.DDL):
			d.ParamPrefix = &uid[i]
			if i+1 > uidLength {
				return nil, nil, nil, errors.Errorf("%s Value must be specified", ddsutils.OUserIDType)
			}
			NextVal := &uid[i+1]
			switch {
			case strings.EqualFold(*NextVal, ddsutils.INCLUDE):
				mark = ddsutils.INCLUDE
			case strings.EqualFold(*NextVal, ddsutils.EXCLUDE):
				mark = ddsutils.EXCLUDE
			}
			i = i + 1
		case strings.EqualFold(uid[i], ddsutils.OBJNAME):
			if i+1 > uidLength {
				return nil, nil, nil, errors.Errorf("%s Value must be specified", ddsutils.OBJNAME)
			}
			if len(ot.owner) > 0 || len(ot.table) > 0 {
				return nil, nil, nil, errors.Errorf("%s Keywords cannot be repeated", ddsutils.OBJNAME)
			}

			NextVal := &uid[i+1]
			// 判断改数据库类型是否支持此选项
			ops := strings.Split(*NextVal, ",")
			for _, op := range ops {
				ind := strings.Index(strings.TrimSpace(op), ".")
				if ind == -1 {
					return nil, nil, nil, errors.Errorf("%s Value must be specified. <database or schema>.<table>", ddsutils.OBJNAME)
				}

				switch *d.parseDBType {
				case ddsutils.Oracle:
					if len(op[:ind]) == 0 {
						return nil, nil, nil, errors.Errorf("%s %s Value owner Name must be specified", ddsutils.OBJNAME, op)
					}
					if strings.HasPrefix(op[:ind], `"`) && strings.HasSuffix(op[:ind], `"`) {
						// 添加用户，如果前后都包含引号，则使用传递的
						ot.owner = strings.Trim(op[:ind], "\"")
					} else if !strings.HasPrefix(op[:ind], `"`) && !strings.HasSuffix(op[:ind], `"`) {
						// 添加用户，如果前后都不包含引号，则转成大写
						ot.owner = strings.ToUpper(op[:ind])
					} else {
						return nil, nil, nil, errors.Errorf("%s Value %s Missing Quotation marks", ddsutils.OBJNAME, op[:ind])
					}

					if len(op[ind+1:]) == 0 {
						return nil, nil, nil, errors.Errorf("%s %s Value table Name must be specified", ddsutils.OBJNAME, op)
					}
					if strings.HasPrefix(op[ind+1:], `"`) && strings.HasSuffix(op[ind+1:], `"`) {
						// 添加用户，如果前后都包含引号，则使用传递的
						ot.table = op[ind+1:]
					} else if !strings.HasPrefix(op[ind+1:], `"`) && !strings.HasSuffix(op[ind+1:], `"`) {
						// 添加用户，如果前后都不包含引号，则转成大写
						ot.table = strings.ToUpper(op[ind+1:])
					} else {
						return nil, nil, nil, errors.Errorf("%s Value %s Missing Quotation marks", ddsutils.OBJNAME, op[ind+1:])
					}
				case ddsutils.MySQL:
					if len(op[:ind]) == 0 {
						return nil, nil, nil, errors.Errorf("%s %s Value owner Name must be specified", ddsutils.OBJNAME, op)
					}
					ot.owner = strings.Trim(op[:ind], `"`)

					if len(op[ind+1:]) == 0 {
						return nil, nil, nil, errors.Errorf("%s %s Value table Name must be specified", ddsutils.OBJNAME, op)
					}
					ot.table = strings.Trim(op[ind+1:], `"`)
				}
			}
			i = i + 1
		case strings.EqualFold(uid[i], ddsutils.OPTYPE):
			if i+1 > uidLength {
				return nil, nil, nil, errors.Errorf("%s Value must be specified", ddsutils.OPTYPE)
			}

			if len(on.opType) > 0 || len(on.opType) > 0 {
				return nil, nil, nil, errors.Errorf("%s Keywords cannot be repeated", ddsutils.OPTYPE)
			}

			NextVal := &uid[i+1]
			// 判断改数据库类型是否支持此选项
			ops := strings.Split(strings.Trim(*NextVal, `"`), ",")
			for _, op := range ops {
				_, ok := d.supportOption.opts[*d.parseDBType].opType[strings.ToUpper(op)]
				if ok {
					on.opType[strings.ToUpper(op)] = strings.ToUpper(op)
				} else {
					if len(op) == 0 {
						return nil, nil, nil, errors.Errorf("%s %s Missing option", ddsutils.OPTYPE, *NextVal)
					} else {
						return nil, nil, nil, errors.Errorf("%s %s %s option<%s> is not supported", *d.parseDBType, ddsutils.OPTYPE, *NextVal, op)
					}

				}
			}
			i = i + 1
		case strings.EqualFold(uid[i], ddsutils.OBJTYPE):
			if i+1 > uidLength {
				return nil, nil, nil, errors.Errorf("%s Value must be specified", ddsutils.OBJTYPE)
			}

			if len(on.objType) > 0 || len(on.objType) > 0 {
				return nil, nil, nil, errors.Errorf("%s Keywords cannot be repeated", ddsutils.OBJTYPE)
			}

			NextVal := &uid[i+1]
			// 判断改数据库类型是否支持此选项
			ops := strings.Split(strings.Trim(*NextVal, `"`), ",")
			for _, op := range ops {
				_, ok := d.supportOption.opts[*d.parseDBType].objType[strings.ToUpper(op)]
				if ok {
					on.objType[strings.ToUpper(op)] = strings.ToUpper(op)
				} else {
					if len(op) == 0 {
						return nil, nil, nil, errors.Errorf("%s %s Missing option", ddsutils.OBJTYPE, *NextVal)
					} else {
						return nil, nil, nil, errors.Errorf("%s %s %s option<%s> is not supported", *d.parseDBType, ddsutils.OBJTYPE, *NextVal, op)
					}
				}

			}
			i = i + 1
		default:
			return nil, nil, nil, errors.Errorf("unknown keyword: %s", uid[i])
		}
	}
	return &mark, &ot, &on, nil
}

func (d *DdlSmt) add(raw *string) error {
	mark, ot, on, err := parseDDLText(raw, d)
	if err != nil {
		return err
	}
	if len(ot.table) == 0 || len(ot.owner) == 0 {
		return errors.Errorf("%s %s Value must be specified", *d.ParamPrefix, ddsutils.OBJNAME)
	}
	var exists bool
	switch *mark {
	case ddsutils.INCLUDE:
		if d.DdlBus.include == nil {
			d.DdlBus.include = append(d.DdlBus.include, map[ownTab]*ObjectNode{*ot: on})
		} else {
			for _, ops := range d.DdlBus.include {
				_, ok := ops[*ot]
				if ok {
					exists = ok
					for _, s2 := range on.opType {
						ops[*ot].opType[s2] = s2
					}

					for _, s2 := range on.objType {
						ops[*ot].objType[s2] = s2
					}

				}
			}
			if !exists {
				d.DdlBus.include = append(d.DdlBus.include, map[ownTab]*ObjectNode{*ot: on})
			}
		}

	case ddsutils.EXCLUDE:
		if d.DdlBus.exclude == nil {
			d.DdlBus.exclude = append(d.DdlBus.exclude, map[ownTab]*ObjectNode{*ot: on})
		} else {
			for _, ops := range d.DdlBus.exclude {
				_, ok := ops[*ot]
				if ok {
					exists = ok
					for _, s2 := range on.opType {
						ops[*ot].opType[s2] = s2
					}

					for _, s2 := range on.objType {
						ops[*ot].objType[s2] = s2
					}
				}
			}
			if !exists {
				d.DdlBus.exclude = append(d.DdlBus.exclude, map[ownTab]*ObjectNode{*ot: on})
			}

		}

	}
	return nil
}

func (d *DdlSmt) initDefault() error {
	return nil
}

func (d *DdlSmt) isType(raw *string, dbType *string, processType *string) error {
	d.init()
	d.parseDBType = dbType
	_, ok := d.supportParams[*dbType][*processType]
	if ok {
		return nil
	}
	return errors.Errorf("The %s %s process does not support this parameter: %s", *dbType, *processType, *raw)
}

func (d *DdlSmt) parse(raw *string) error {
	mark, ot, on, err := parseDDLText(raw, d)
	if err != nil {
		return err
	}
	if len(ot.table) == 0 || len(ot.owner) == 0 {
		return errors.Errorf("%s %s Value must be specified", *d.ParamPrefix, ddsutils.OBJNAME)
	}
	switch *mark {
	case ddsutils.INCLUDE:
		d.DdlBus.include = append(d.DdlBus.include, map[ownTab]*ObjectNode{*ot: on})
	case ddsutils.EXCLUDE:
		d.DdlBus.exclude = append(d.DdlBus.exclude, map[ownTab]*ObjectNode{*ot: on})
	}
	return nil
}

type DdlSmtSet struct {
	dds *DdlSmt
}

func (d *DdlSmtSet) MarshalJson() ([]byte, error) {
	var djSet []DdlJson
	var dj DdlJson
	dj.Type = &ddsutils.DDL
	dj.Range = &ddsutils.INCLUDE
	for _, m := range d.dds.DdlBus.include {
		for tab, node := range m {
			dj.Owner = &tab.owner
			dj.Table = &tab.table
			for _, v := range node.opType {
				dj.OpType = append(dj.OpType, v)
			}
			for _, v := range node.objType {
				dj.ObjType = append(dj.ObjType, v)
			}
		}
		djSet = append(djSet, dj)
	}
	xj, err := json.Marshal(djSet)
	return xj, err
}

var DdlSmtSetBus DdlSmtSet

func (d *DdlSmtSet) Init() {
	d.dds = new(DdlSmt)
}

func (d *DdlSmtSet) Add(raw *string) error {
	return d.dds.add(raw)
}

func (d *DdlSmtSet) ListParamText() string {
	return d.dds.put()
}

func (d *DdlSmtSet) GetParam() interface{} {
	return d.dds
}

func (d *DdlSmtSet) Registry() map[string]Parameter {
	d.Init()
	return map[string]Parameter{ddsutils.DDL: d.dds}
}
