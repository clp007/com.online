package com.atguigu.util

import java.sql.{Connection, PreparedStatement, ResultSet}

trait QueryCallback{
    def process(result:ResultSet)
}

class SqlProxy {

    private var result: ResultSet=_
    private var prepare :PreparedStatement=_

    /**
      * 执行修改语句
      * @param conn 连接
      * @param sql 语句
      * @param params 要传的参数
      */

    def executeUpdate(conn:Connection,sql:String,params:Array[Any])={

        var retn =0
        try {
            //预编译sql
            prepare=conn.prepareStatement(sql)
            if(params!=null && params.length>0){
                for (e<- 0 until params.length) {

                    //p1 :在sql的位置，从1开始，把参数传给sql
                    prepare.setObject(e+1,params(e))
                }
            }
            //返回值
            retn=prepare.executeUpdate()
        }catch {
            case e:Exception=>e.printStackTrace()
        }
        retn
    }


    def executeQuery(conn:Connection,sql:String,params:Array[Any],call:QueryCallback)={

        result=null

        try{

            val prepare: PreparedStatement = conn.prepareStatement(sql)

            if (params!=null&& params.length!=0){

                for (i <- 0 until params.length) {

                    prepare.setObject(i+1,params(0))
                }
            }

            val result: ResultSet = prepare.executeQuery()
            //返回结果集
            call.process(result)

        }catch {
            case e:Exception =>e.printStackTrace()
        }

    }


    def shutdown(conn:Connection)=DataSourceUtil.closeResource(result,prepare,conn)
}
