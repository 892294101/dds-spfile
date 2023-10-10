package ddsspfile

import (
	"encoding/json"
	"fmt"
	"github.com/892294101/dds-utils"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

type TrailAttribute struct {
	dir       *string
	sizeKey   *string
	SizeValue *int
	SizeUnit  *string
	keepKey   *string
	keepValue *int
	keepUnit  *string
}

func (t *TrailAttribute) setDir(s *string) { t.dir = s }
func (t *TrailAttribute) GetDir() *string  { return t.dir }

func (t *TrailAttribute) setSizeKey(s *string) { t.sizeKey = s }
func (t *TrailAttribute) GetSizeKey() *string  { return t.sizeKey }

func (t *TrailAttribute) setSizeValue(s *int) { t.SizeValue = s }
func (t *TrailAttribute) GetSizeValue() *int  { return t.SizeValue }

func (t *TrailAttribute) setSizeUnit(s *string) { t.SizeUnit = s }
func (t *TrailAttribute) GetSizeUnit() *string  { return t.SizeUnit }

func (t *TrailAttribute) setKeepKey(s *string) { t.keepKey = s }
func (t *TrailAttribute) GetKeepKey() *string  { return t.keepKey }

func (t *TrailAttribute) setKeepVal(s *int) { t.keepValue = s }
func (t *TrailAttribute) GetKeepVal() *int  { return t.keepValue }

func (t *TrailAttribute) setKeepUnit(s *string) { t.keepUnit = s }
func (t *TrailAttribute) GetKeepUnit() *string  { return t.keepUnit }

func (t *TrailAttribute) GetTrail() *TrailAttribute { return t }

type TrailDir struct {
	supportParams     map[string]map[string]string
	paramPrefix       *string
	DirTrailAttribute *TrailAttribute
}

func (t *TrailDir) put() string {
	return fmt.Sprintf("%s %s %s %d %s %d %s\n", *t.paramPrefix, *t.DirTrailAttribute.GetDir(), *t.DirTrailAttribute.GetSizeKey(), *t.DirTrailAttribute.GetSizeValue(), *t.DirTrailAttribute.GetKeepKey(), *t.DirTrailAttribute.GetKeepVal(), *t.DirTrailAttribute.GetKeepUnit())
}

// 初始化参数可以支持的数据库和进程
func (t *TrailDir) init() {
	t.supportParams = map[string]map[string]string{
		ddsutils.MySQL: {
			ddsutils.Extract:  ddsutils.Extract,
			ddsutils.Replicat: ddsutils.Replicat,
		},
		ddsutils.Oracle: {
			ddsutils.Extract:  ddsutils.Extract,
			ddsutils.Replicat: ddsutils.Replicat,
		},
	}
}

func (t *TrailDir) initDefault() error {
	return nil
}

func (t *TrailDir) isType(raw *string, dbType *string, processType *string) error {
	t.init()
	_, ok := t.supportParams[*dbType][*processType]
	if ok {
		return nil
	}
	return errors.Errorf("The %s %s process does not support this parameter: %s", *dbType, *processType, *raw)
}

func (t *TrailDir) parse(raw *string) error {
	trail := ddsutils.TrimKeySpace(strings.Split(*raw, " "))
	trailLength := len(trail) - 1
	for i := 0; i < len(trail); i++ {
		switch {
		case strings.EqualFold(trail[i], ddsutils.TrailDirType):
			t.paramPrefix = &trail[i]
			if i+1 > trailLength {
				return errors.Errorf("%s Value must be specified", trail[i])
			}
			NextVal := &trail[i+1]
			if ddsutils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if t.DirTrailAttribute.dir != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			t.DirTrailAttribute.dir = NextVal
			i += 1

		case strings.EqualFold(trail[i], ddsutils.TrailSizeKey):

			t.DirTrailAttribute.setSizeKey(&trail[i])
			if i+1 > trailLength {
				return errors.Errorf("%s Value must be specified", ddsutils.TrailSizeKey)
			}
			NextVal := &trail[i+1]
			if ddsutils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if t.DirTrailAttribute.SizeValue != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			s, err := strconv.Atoi(*NextVal)
			if err != nil {
				return errors.Errorf("%s Value is not a numeric integer: %s", trail[i], *NextVal)
			}

			if s < ddsutils.DefaultTrailMinSize {
				return errors.Errorf("%s Value %s cannot be less than the minimum size %d", trail[i], *NextVal, ddsutils.DefaultTrailMinSize)
			}

			t.DirTrailAttribute.setSizeValue(&s)
			i += 1
		case strings.EqualFold(trail[i], ddsutils.TrailKeepKey):

			t.DirTrailAttribute.setKeepKey(&trail[i])
			if i+1 > trailLength {
				return errors.Errorf("%s Value must be specified", trail[i])
			}
			NextVal := &trail[i+1]
			if ddsutils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if t.DirTrailAttribute.keepValue != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}
			s, err := strconv.Atoi(*NextVal)
			if err != nil {
				return errors.Errorf("%s Value is not a numeric integer: %s", trail[i], *NextVal)
			}
			t.DirTrailAttribute.setKeepVal(&s)
			i += 1

			if i+1 > trailLength {
				return errors.Errorf("%s unit Value must be specified", *t.DirTrailAttribute.GetKeepKey())
			}
			NextVal = &trail[i+1]
			if ddsutils.KeyCheck(NextVal) {
				return errors.Errorf("keywords cannot be used: %s", *NextVal)
			}
			if t.DirTrailAttribute.GetKeepUnit() != nil {
				return errors.Errorf("Parameters cannot be repeated: %s", *NextVal)
			}

			if strings.EqualFold(*NextVal, ddsutils.MB) || strings.EqualFold(*NextVal, ddsutils.GB) || strings.EqualFold(*NextVal, ddsutils.DAY) {
				t.DirTrailAttribute.setKeepUnit(NextVal)
				i += 1
			} else {
				return errors.Errorf("%s unit Value Only supported %s/%s/%s", *t.DirTrailAttribute.GetKeepKey(), ddsutils.MB, ddsutils.GB, ddsutils.DAY)
			}
		default:
			return errors.Errorf("unknown parameter: %s", trail[i])
		}
	}

	if t.DirTrailAttribute == nil {
		t.DirTrailAttribute = &TrailAttribute{
			sizeKey:   &ddsutils.TrailSizeKey,
			SizeValue: &ddsutils.DefaultTrailMaxSize,
			SizeUnit:  &ddsutils.MB,
			keepKey:   &ddsutils.TrailKeepKey,
			keepValue: &ddsutils.DefaultTrailKeepValue,
			keepUnit:  &ddsutils.DAY,
		}
	} else {
		if t.DirTrailAttribute.GetSizeValue() == nil {
			t.DirTrailAttribute.setSizeKey(&ddsutils.TrailSizeKey)
			t.DirTrailAttribute.setSizeValue(&ddsutils.DefaultTrailMaxSize)
			t.DirTrailAttribute.setSizeUnit(&ddsutils.MB)
		}
		if t.DirTrailAttribute.GetKeepVal() == nil {
			t.DirTrailAttribute.setKeepKey(&ddsutils.TrailKeepKey)
			t.DirTrailAttribute.setKeepVal(&ddsutils.DefaultTrailKeepValue)
			t.DirTrailAttribute.setKeepUnit(&ddsutils.DAY)
		}
		if t.DirTrailAttribute.GetSizeUnit() == nil {
			t.DirTrailAttribute.setSizeUnit(&ddsutils.MB)
		}
	}

	return nil
}

func (t *TrailDir) add(raw *string) error {

	return nil
}

type trailDirSet struct {
	trailDir *TrailDir
}

func (t *trailDirSet) MarshalJson() ([]byte, error) {
	var te TrailDirJson
	te.Type = t.trailDir.paramPrefix
	te.Dir = t.trailDir.DirTrailAttribute.GetDir()
	te.Size = t.trailDir.DirTrailAttribute.GetSizeValue()
	te.SizeUnit = t.trailDir.DirTrailAttribute.GetSizeUnit()
	te.Keep = t.trailDir.DirTrailAttribute.GetKeepVal()
	te.KeepUnit = t.trailDir.DirTrailAttribute.GetKeepUnit()
	tej, err := json.Marshal(te)
	return tej, err
}

var trailDirBus trailDirSet

func (t *trailDirSet) Init() {
	t.trailDir = new(TrailDir)
	t.trailDir.DirTrailAttribute = new(TrailAttribute)
}

func (t *trailDirSet) Add(raw *string) error {
	return nil
}

func (t *trailDirSet) ListParamText() string {
	return t.trailDir.put()
}

func (t *trailDirSet) GetParam() interface{} {
	return t.trailDir
}

func (t *trailDirSet) Registry() map[string]Parameter {
	t.Init()
	return map[string]Parameter{ddsutils.TrailDirType: t.trailDir}
}
