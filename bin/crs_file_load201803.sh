#!/usr/bin/ksh
#############################################################################
#
# FILE NAME:      crs_file_load.sh
# DESCRIPTION:    to run whole batch for project CRS
#
#
# MODIFICATION HISTORY:
# Name           Date               Description
#========  ==========   =================================================
# bingo     2017-12-20   Created.
#
#############################################################################
#
# ACTION Show usage
#
  fun_show_usage()
  {
     echo ""
     echo "example"  : crs_file_load.sh 20171220 crs 1 1
     echo "USAGE     : crs_file_load.sh <period> <jobseq> <areano> <jobid>"
     echo "KSH_DIR   : Program loacation"
     echo "PERIOD    : ETL AC Date [string 8]"
     echo "JOBSEQ    : ETL Job sequence no"
     echo "AREANO    : ETL Area no"
     echo "JOBID     : ETL Job ID"
  }





  KSH_DIR=`dirname $0`




#参数赋值
#set the param
#
USRPWD=`$JOBCTL_BIN_PATH/dbconnect get userid|awk '{usri=index($0,"=")
                             usrj=index($0,",")
                             usr=substr($0,usri+1,usrj-usri-1)
                             tmp=substr($0,usrj+1)
                             pwdj=index(tmp,"=")
                             pwd =substr(tmp,pwdj+1)
                             printf("%s/%s\n",usr,pwd) }'`

  varPeriod=$1
  varJobseq=$2
  varAerano=$3
  varJobid=$4
  varDirData="$HOME/ZRTP_JP/data/receive"
  varDirCtl="$HOME/ZRTP_JP/ctl"
  varLog="$HOME/ZRTP_JP/log/crs_oibs.log"
  varDBName=$ORACLE_SID
  varDBUser=`$JOBCTL_BIN_PATH/dbconnect get userid|awk '{usri=index($0,"=")
                             usrj=index($0,",")
                             usr=substr($0,usri+1,usrj-usri-1)
                             printf("%s",usr) }'`
  varDBPass=`$JOBCTL_BIN_PATH/dbconnect get userid|awk '{usrj=index($0,",")
                             tmp=substr($0,usrj+1)
                             pwdj=index(tmp,"=")
                             pwd =substr(tmp,pwdj+1)
                             printf("%s",pwd) }'`

  varArrayTabnm[0]='CRS_LEGAL_CUSTOMER'

  varArrayTabnm[1]='CRS_LEGAL_CUSTOMER_CONTROLLER'

  varArrayTabnm[2]='CRS_LEGAL_CUSTOMER_TAX_INFO'

  varArrayTabnm[3]='CRS_PERSONAL_CUSTOMER_TAX_INFO'

#备份目录建立
#back
  if [ ! -d ${varDirData}/backup ];then

     mkdir -p ${varDirData}/backup

  fi
 


##########################################################
#cd the dir of ksh
###########################################################

  if [ "${KSH_DIR}_NULL" != "_NULL" ] && [ -d ${KSH_DIR} ];then

     cd ${KSH_DIR}

  else

     echo `date +%Y-%m-%d" "%H:%M:%S`" Program Location not found "| tee -a ${varLog}
     fun_show_usage
     exit 1

  fi

###############################################################
#数据库连接测试
# db connection test
###############################################################
    varSql="SELECT SYSDATE FROM DUAL;"
    varRes=`sqlplus -S ${varDBUser}/${varDBPass}@${varDBName} <<EOF >/dev/null
            set ECHO OFF
            set FEEDBACK OFF
            set VERIFY OFF
            set HEADING OFF
            set TERMOUT OFF
            set TRIMOUT ON
            set TRIMSPOOL ON
            set PAGESIZE 0
            set LINESIZE 2048
            set SERVEROUTPUT ON
            WHENEVER SQLERROR EXIT SQL.SQLCODE;
            ${varSql}
            EXIT SQL.SQLCODE
            EOF`
    varRet=$?
    if [ ${varRet} -ne 0 ];then
       varLogMsg=`echo "Database Connection [ Error ]: ${varRes}"|xargs`
       echo `date +%Y-%m-%d" "%H:%M:%S`" ERROR" "${varLogMsg}" | tee -a ${varLog}
       exit 1

    fi
    echo `date +%Y-%m-%d" "%H:%M:%S`" db connection [ success ]" | tee -a ${varLog}



#
# produrct filelist
#
#    FileList=`ls ${varDirData} | grep "^CRS.*RRS.gz.*" | tee ${varDirData}/crslist.ls`
#    varRet=$?
#    if [ ${varRet} -ne 0 ];then
#       varLogMsg=`echo "read filelist to ${varDirData}/crslist.ls`
#       echo `date +%Y-%m-%d" "%H:%M:%S`" ERROR" "${varLogMsg}" | tee -a ${varLog}
#       exit 1
#
#    fi
#    echo `date +%Y-%m-%d" "%H:%M:%S`" [ success ] read FileList to ${varDirData}/crslist.ls " | tee -a ${varLog}
#
#    varFileRow=` grep -c '.*' ${varDirData}/crslist.ls `


#循环次序与crslist文件行数初始化赋值
    varLoopCnt=0
    varFileRow=0
    
########################################################################################    
# 循环主体
########################################################################################
    while [ ${varFileRow} -lt 4 ]; do

      echo `date +%Y-%m-%d" "%H:%M:%S`" [ check ] FileList total $filerow not -gt 4 ,sleep 60s and try again " | tee -a ${varLog}
      
######休眠时间定义
      sleep 60

##########################################
# produrct filelist
#
###########################################
#读取数据文件目录下的文件名，输出到crslist.ls文件
###########################################
      FileList=`ls ${varDirData} | grep "^CRS.*RRS.gz.*" | tee ${varDirData}/crslist.ls`
      varRet=$?
      if [ ${varRet} -ne 0 ];then
         varLogMsg=`echo "read filelist loop to ${varDirData}/crslist.ls`
         echo `date +%Y-%m-%d" "%H:%M:%S`" [ check ] ERROR " "${varLogMsg}" | tee -a ${varLog}
         exit 1
      fi
      echo `date +%Y-%m-%d" "%H:%M:%S`" [ check ] success read FileList loop to ${varDirData}/crslist.ls " | tee -a ${varLog}
      
######################################
#读取crslist.ls的行数--数据文件到了几个文件
#######################################
      varFileRow=` grep -c '.*' ${varDirData}/crslist.ls `
      echo `date +%Y-%m-%d" "%H:%M:%S`" [ check ] file row is ${varFileRow} " | tee -a ${varLog}
      echo `date +%Y-%m-%d" "%H:%M:%S`" [ check ] file list is ${FileList} " | tee -a ${varLog}

######################################
#循环次序自增
######################################
      varLoopCnt=`expr ${varLoopCnt} + 1`
      if [ ${varLoopCnt} -gt 300 ];then

        echo `date +%Y-%m-%d" "%H:%M:%S`"[ check ] ERROR" "loop ${varLoopCnt} time out  , exit 1 " | tee -a ${varLog}
        exit 1

      fi

    done



  n=0
  ###################################
  #循环加载文件主体
  ###################################
  for varFileName in ${FileList}
  do
  #获取表名，控制文件名，数据文件名
    varTabnm=` echo ${varFileName} | awk -F '.' '{print $1}' `
    varDirTabctl=${varDirCtl}/${varTabnm}.ctl
    varDirTabdat=${varDirData}/${varTabnm}.RRS

    echo `date +%Y-%m-%d" "%H:%M:%S`" $n ###########################################################" | tee -a ${varLog}

#复制数据文件，去日期后缀
    varRes=`cp ${varDirData}/${varFileName} ${varDirTabdat}.gz`
    if [ ${varRet} -ne 0 ];then

       echo `date +%Y-%m-%d" "%H:%M:%S`" [ copy ] ${varDirTabdat}.gz.${varPeriod} to ${varDirTabdat}.gz [ fail ]" | tee -a ${varLog}
       exit 1

    fi
    echo `date +%Y-%m-%d" "%H:%M:%S`" [ copy ] ${varDirTabdat}.gz.${varPeriod} to ${varDirTabdat}.gz [ success ]" | tee -a ${varLog}

#解压数据文件
    varRes=`gunzip -f ${varDirTabdat}.gz`
    if [ ${varRet} -ne 0 ];then

       echo `date +%Y-%m-%d" "%H:%M:%S`" [ gunzip ] ${varDirTabdat}.gz [ fail ]" | tee -a ${varLog}
       exit 1

    fi
    echo `date +%Y-%m-%d" "%H:%M:%S`" [ gunzip ] ${varDirTabdat}.gz [ success ]" | tee -a ${varLog}
    
#SQL*ldr加载数据文件
    echo `date +%Y-%m-%d" "%H:%M:%S`" [ load ] ${varTabnm} file [ start ]" | tee -a ${varLog}
    varRes=`sqlldr userid="${varDBUser}/${varDBPass}@${varDBName}" control="${varDirTabctl}" data="${varDirTabdat}" log="$HOME/ZRTP_JP/log/${varTabnm}.log"`
    varRet=$?
    if [ ${varRet} -ne 0 ];then

       echo `date +%Y-%m-%d" "%H:%M:%S`" [ load ] ${varTabnm} file ${varDirTabdat} [ fail ]" | tee -a ${varLog}
       exit 1

    fi
    echo `date +%Y-%m-%d" "%H:%M:%S`" [ load ] ${varTabnm} file [ success ]" | tee -a ${varLog}

#加载完成，移动数据文件到备份目录
    mv -f ${varDirData}/${varFileName} ${varDirData}/backup
    varRet=$?
    if [ ${varRet} -ne 0 ];then

       echo `date +%Y-%m-%d" "%H:%M:%S`" [ move ] ${varFileName} file to ${varDirTabdat}/backup [ fail ]" | tee -a ${varLog}
       exit 1

    fi
    echo `date +%Y-%m-%d" "%H:%M:%S`" [ move ] ${varFileName} file [ success ]" | tee -a ${varLog}

    echo " " | tee -a ${varLog}
	n=`expr $n + 1`
 done

exit 0
