%let pgm=utl-compute-the-mean-by-group-using-sas-wps-python-r-native-code-and-sql;

Compute the mean by group using sas wps python r native code and sql

github
https://tinyurl.com/2ujvskym
https://github.com/rogerjdeangelis/utl-compute-the-mean-by-group-using-sas-wps-python-r-native-code-and-sql

StackOverflow R

How can I use group_by and mutate together to calculate the means of certain columns?

https://tinyurl.com/57nbh3hd
https://stackoverflow.com/questions/76756044/how-can-i-use-group-by-and-mutate-together-to-calculate-the-means-of-certain-col

  SOLUTIONS

       1. wps/sas sql (WPS proc means, summary, tabulate, corresp ... are simpler solutions)
       2. r native
       3. wps/r sql I used a scratch environment variable becase I could not figure out how
          to pass SAS macro or macro variable inside proc r. Also uses sql arrays.
          Simpler if python code not inside the parent sas code.
       4. wps/r sql without do_over
       5. wps/python sql I used a scratch environment variable becase I could not figure out how
          to pass SAS macro or macro variable inside proc r. Also uses sql arrays.
          Simpler if python code not inside the parent sas code.
       6. Python native (took me a long time to figure out I neede "reset_index()"
          want=have.groupby(['SEX','PHENOTYPE']).mean().reset_index();

  How can I use group_by and mutate together to calculate the means of certain columns?

  WPS, SAS, R and Python gererate this code

      select
        Sex
       ,Phenotype
       ,avg(mop_amygdala)   as mop_amygdala
       ,avg(mop_thalamus)   as mop_thalamus
       ,avg(mop_cerebellum) as mop_cerebellum
       ,avg(moq_cortex)     as moq_cortex
      from
        have
      group
        by  Sex
           ,Phenotype

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/
options validvarname=upcase;
data sd1.have;informat
SEX $1.
PHENOTYPE $12.
MOP_AMYGDALA 2.
MOP_THALAMUS 2.
MOP_CEREBELLUM 2.
MOQ_CORTEX 2.
MOQ_STRIATUM 3.
;input
SEX PHENOTYPE MOP_AMYGDALA MOP_THALAMUS MOP_CEREBELLUM MOQ_CORTEX MOQ_STRIATUM;
cards4;
F Control 10 19 34 2 100
F Experimental 15 12 45 5 101
M Experimental 2 4 67 6 102
M Control 6 4 78 17 106
F Control 8 6 99 2 200
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from SD1.HAVE total obs=5                                                                                 */
/*                                 MOP_        MOP_                         MOQ_       MOQ_                               */
/* Obs    SEX    PHENOTYPE       AMYGDALA    THALAMUS    MOP_CEREBELLUM    CORTEX    STRIATUM                             */
/*                                                                                                                        */
/*  1      F     Control            10          19             34             2         100                               */
/*  2      F     Experimental       15          12             45             5         101                               */
/*  3      M     Experimental        2           4             67             6         102                               */
/*  4      M     Control             6           4             78            17         106                               */
/*  5      F     Control             8           6             99             2         200                               */
/*                                                                                                                        */
/*                                                                                                                        */
/*                                                                                                                        */
/* OUTPUT means by sex and PHENOTYPE                                                                                      */
/*                                                                                                                        */
/* Obs    SEX     PHENOTYPE      MOP_AMYGDALA    MOP_THALAMUS    MOP_CEREBELLUM    MOQ_CORTEX    MOQ_STRIATUM             */
/*                                                                                                                        */
/*  1      F     Control               9             12.5             66.5              2             150   (200+100)/2   */
/*  2      F     Experimental         15             12.0             45.0              5             101                 */
/*  3      M     Control               6              4.0             78.0             17             106                 */
/*  4      M     Experimental          2              4.0             67.0              6             102                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                         __                         _
/ | __      ___ __  ___   / /__  __ _ ___   ___  __ _| |
| | \ \ /\ / / `_ \/ __| / / __|/ _` / __| / __|/ _` | |
| |  \ V  V /| |_) \__ \/ /\__ \ (_| \__ \ \__ \ (_| | |
|_|   \_/\_/ | .__/|___/_/ |___/\__,_|___/ |___/\__, |_|
             |_|                                   |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

libname sd1 "d:/sd1";

%array(_var,values=%utl_varlist(sd1.have,keep=mo:));

%put &=_var3;
%put &=_varn;

proc sql;
  create
    table sd1.want as
  select
    Sex
   ,Phenotype
   ,%do_over(_var,phrase=mean(?) as ?,between=comma)
  from
    sd1.have
  group
    by  Sex
       ,Phenotype
;quit;

proc print data=sd1.want;
run;quit;


/*
__      ___ __  ___
\ \ /\ / / `_ \/ __|
 \ V  V /| |_) \__ \
  \_/\_/ | .__/|___/
         |_|
*/

%utl_submit_wps64x('
options sasautos=("c:/otowps" sasautos);
%utlopts;
libname sd1 "d:/sd1";
%array(_var,values=%utl_varlist(sd1.have,keep=mo:));
%put &=_var3;
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want <- "
  select
    Sex
   ,Phenotype
   ,%do_over(_var,phrase=mean(?) as ?,between=comma)
  from
    have
  group
    by  Sex
       ,Phenotype
  ";;
want;
endsubmit;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/*  Obs    SEX     PHENOTYPE      MOP_AMYGDALA    MOP_THALAMUS    MOP_CEREBELLUM    MOQ_CORTEX    MOQ_STRIATUM            */
/*                                                                                                                        */
/*   1      F     Control               9             12.5             66.5              2             150                */
/*   2      F     Experimental         15             12.0             45.0              5             101                */
/*   3      M     Control               6              4.0             78.0             17             106                */
/*   4      M     Experimental          2              4.0             67.0              6             102                */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                       _   _
|___ \   _ __   _ __   __ _| |_(_)_   _____
  __) | | `__| | `_ \ / _` | __| \ \ / / _ \
 / __/  | |    | | | | (_| | |_| |\ V /  __/
|_____| |_|    |_| |_|\__,_|\__|_| \_/ \___|

*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

/*---- update wps config -set SASAUTOS ('c:/otowps' '!wpshome\sasmacro') ----*/

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(tidyverse);
want <- have %>% summarise(across(starts_with("MO"), mean),.by = c(SEX, PHENOTYPE));
want;
endsubmit;
import data=sd1.want r=have;
run;quit;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/*   SEX    PHENOTYPE MOP_AMYGDALA MOP_THALAMUS MOP_CEREBELLUM MOQ_CORTEX MOQ_STRIATUM                                    */
/* 1   F      Control            9         12.5           66.5          2          150                                    */
/* 2   F Experimental           15         12.0           45.0          5          101                                    */
/* 3   M Experimental            2          4.0           67.0          6          102                                    */
/* 4   M      Control            6          4.0           78.0         17          106                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                         __                _
|___ /  __      ___ __  ___   / / __   ___  __ _| |
  |_ \  \ \ /\ / / `_ \/ __| / / `__| / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \/ /| |    \__ \ (_| | |
|____/    \_/\_/ | .__/|___/_/ |_|    |___/\__, |_|
                 |_|                          |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

/*----  I am passing sql code to r usung an eviroment variable           ----*/

%utl_submit_wps64x('
libname sd1 "d:/sd1";
options sasautos=("c:/otowps" sasautos);
%array(_var,values=%utl_varlist(sd1.have,keep=mo:));
data _null_;
 length env $1034;
 env=compbl(resolve("
  select
    Sex
   ,Phenotype
   ,%do_over(_var,phrase=avg(?) as ?,between=comma)
  from
    have
  group
    by  Sex
       ,Phenotype"));
  call symputx("env",env);
  put env;
run;quit;
options set=scratch "&env";
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
sqlcde<-Sys.getenv(c("SCRATCH"));
print(sqlcde,width = 1024);
want <- sqldf(sqlcde);
want;
endsubmit;
import data=sd1.want r=want;
run;quit;
proc print data=sd1.want;
run;quit;
');

proc print data=sd1.want;
run;quit;

/*  _                          __                _                    _
| || |  __      ___ __  ___   / / __   ___  __ _| |  _ __   ___    __| | ___     _____   _____ _ __
| || |_ \ \ /\ / / `_ \/ __| / / `__| / __|/ _` | | | `_ \ / _ \  / _` |/ _ \   / _ \ \ / / _ \ `__|
|__   _| \ V  V /| |_) \__ \/ /| |    \__ \ (_| | | | | | | (_) || (_| | (_) | | (_) \ V /  __/ |
   |_|    \_/\_/ | .__/|___/_/ |_|    |___/\__, |_| |_| |_|\___/  \__,_|\___/___\___/ \_/ \___|_|
                 |_|                          |_|                          |_____|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want <- sqldf("
      select
        Sex
       ,Phenotype
       ,avg(mop_amygdala)   as mop_amygdala
       ,avg(mop_thalamus)   as mop_thalamus
       ,avg(mop_cerebellum) as mop_cerebellum
       ,avg(moq_cortex)     as moq_cortex
      from
        have
      group
        by  Sex
           ,Phenotype");
want;
endsubmit;
import data=sd1.want r=want;
run;quit;
proc print data=sd1.want;
run;quit;
');

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* R The WPS System                                                                                                       */
/*                                                                                                                        */
/*   SEX    PHENOTYPE MOP_AMYGDALA MOP_THALAMUS MOP_CEREBELLUM MOQ_CORTEX MOQ_STRIATUM                                    */
/* 1   F      Control            9         12.5           66.5          2          150                                    */
/* 2   F Experimental           15         12.0           45.0          5          101                                    */
/* 3   M      Control            6          4.0           78.0         17          106                                    */
/* 4   M Experimental            2          4.0           67.0          6          102                                    */
/*                                                                                                                        */
/* WPS                                                                                                                    */
/*                                                                                                                        */
/* Obs    SEX     PHENOTYPE      MOP_AMYGDALA    MOP_THALAMUS    MOP_CEREBELLUM    MOQ_CORTEX    MOQ_STRIATUM             */
/*                                                                                                                        */
/*  1      F     Control               9             12.5             66.5              2             150                 */
/*  2      F     Experimental         15             12.0             45.0              5             101                 */
/*  3      M     Control               6              4.0             78.0             17             106                 */
/*  4      M     Experimental          2              4.0             67.0              6             102                 */
/*                                                                                                                        */
/*                                                                                                                         */
/**************************************************************************************************************************/

/*___                          __           _   _                            _
| ___|  __      ___ __  ___   / / __  _   _| |_| |__   ___  _ __   ___  __ _| |
|___ \  \ \ /\ / / `_ \/ __| / / `_ \| | | | __| `_ \ / _ \| `_ \ / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \/ /| |_) | |_| | |_| | | | (_) | | | |\__ \ (_| | |
|____/    \_/\_/ | .__/|___/_/ | .__/ \__, |\__|_| |_|\___/|_| |_||___/\__, |_|
                 |_|           |_|    |___/                               |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
options sasautos=("c:/otowps" sasautos);
%array(_var,values=%utl_varlist(sd1.have,keep=mo:));
data _null_;
 length env $1034;
 env=compbl(resolve("
  select
    Sex
   ,Phenotype
   ,%do_over(_var,phrase=avg(?) as ?,between=comma)
  from
    have
  group
    by  Sex
       ,Phenotype"));
  call symputx("env",env);
run;quit;
options set=scratch "&env";
proc python;
export data=sd1.have python=have;
submit;
import os;
from os import path;
import pandas as pd;
import numpy as np;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
mysql = lambda q: sqldf(q, globals());
sqlcde=os.environ["SCRATCH"];
want = pdsql(sqlcde);
print(want);
endsubmit;
import data=sd1.want python=want;
run;quit;
proc print data=sd1.want;
run;quit;
');

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs    SEX     PHENOTYPE      MOP_AMYGDALA    MOP_THALAMUS    MOP_CEREBELLUM    MOQ_CORTEX    MOQ_STRIATUM             */
/*                                                                                                                        */
/*  1      F     Control               9             12.5             66.5              2             150                 */
/*  2      F     Experimental         15             12.0             45.0              5             101                 */
/*  3      M     Control               6              4.0             78.0             17             106                 */
/*  4      M     Experimental          2              4.0             67.0              6             102                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*           _   _                               _   _
 _ __  _   _| |_| |__   ___  _ __    _ __   __ _| |_(_)_   _____
| `_ \| | | | __| `_ \ / _ \| `_ \  | `_ \ / _` | __| \ \ / / _ \
| |_) | |_| | |_| | | | (_) | | | | | | | | (_| | |_| |\ V /  __/
| .__/ \__, |\__|_| |_|\___/|_| |_| |_| |_|\__,_|\__|_| \_/ \___|
|_|    |___/
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;


%utl_submit_wps64x("
libname sd1 'd:/sd1';
 proc python;
 export data=sd1.have python=have;
 submit;
 print(have);
 want=have.groupby(['SEX','PHENOTYPE']).mean().reset_index();
 want.info();
 print(want);
 endsubmit;
 import data=sd1.want python=want;
 run;quit;
 proc print data=sd1.want;
 run;quit;
");

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/*   SEX     PHENOTYPE  MOP_AMYGDALA  ...  MOP_CEREBELLUM  MOQ_CORTEX  MOQ_STRIATUM                                       */
/* 0   F  Control                9.0  ...            66.5         2.0         150.0                                       */
/* 1   F  Experimental          15.0  ...            45.0         5.0         101.0                                       */
/* 2   M  Control                6.0  ...            78.0        17.0         106.0                                       */
/* 3   M  Experimental           2.0  ...            67.0         6.0         102.0                                       */
/*                                                                                                                        */
/* [4 rows x 7 columns]                                                                                                   */
/*                                                                                                                        */
/*                                                                                                                        */
/*                                                                                                                        */
/* Obs    SEX     PHENOTYPE      MOP_AMYGDALA    MOP_THALAMUS    MOP_CEREBELLUM    MOQ_CORTEX    MOQ_STRIATUM             */
/*                                                                                                                        */
/*  1      F     Control               9             12.5             66.5              2             150                 */
/*  2      F     Experimental         15             12.0             45.0              5             101                 */
/*  3      M     Control               6              4.0             78.0             17             106                 */
/*  4      M     Experimental          2              4.0             67.0              6             102                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
