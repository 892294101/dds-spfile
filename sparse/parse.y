%{
package sparser

import (
	"strings"
	"github.com/dds/spfile"
)

%}



%type   <statement>
DdlStmt           "DDL statement"

%%

DdlStmt:
	"DDL" IncludeOption DdlRange "OBJNAME" ObjNameValue "OPTYPE" OpTypeValue "OBJTYPE" ObjTypeValue
	{
		$$ = &spfile.DdlSmt{IfExists: $3.(bool), Name: model.NewCIStr($4)}
	}

IncludeOption:
|	"INCLUDE"
	{
		$$ = spfile.INCLUDE
	}
|	"EXCLUDE"
	{
		$$ = spfile.EXCLUDE
	}

DdlRange:
|	"ALL"
	{
		$$ = spfile.ALL
	}
|	"MAPPED"
	{
		$$ = spfile.MAPPED
	}


%%



