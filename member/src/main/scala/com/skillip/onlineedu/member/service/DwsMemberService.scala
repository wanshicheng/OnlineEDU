package com.skillip.onlineedu.member.service

import com.skillip.onlineedu.member.bean.{MemberZipper, MemberZipperResult}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DwsMemberService {
  def importMemberUseApi(spark: SparkSession, dt: String) = {
    import spark.implicits._

  }

  def importMember(spark: SparkSession, time: String) = {
    import spark.implicits._
    spark.sql(
      s"""
         |select
         |	uid,
         |	first(ad_id),
         |	first(fullname),
         |	first(iconurl),
         |	first(lastlogin),
         |	first(mailaddr),
         |	first(memberlevel),
         |	first(password),
         |	sum(cast(paymoney as decimal(10,4))),
         |	first(phone),
         |	first(qq),
         |	first(register),
         |	first(regupdatetime),
         |	first(unitname),
         |	first(userip),
         |	first(zipcode),
         |    first(appkey),
         |    first(appregurl),
         |    first(bdp_uuid),
         |    first(reg_createtime),
         |    first(domain),
         |    first(isranreg),
         |    first(regsource),
         |    first(regsourcename),
         |    first(adname),
         |    first(siteid),
         |    first(sitename),
         |    first(siteurl),
         |    first(site_delete),
         |    first(site_createtime),
         |    first(site_creator),
         |    first(vip_id),
         |    max(vip_level),
         |    min(vip_start_time),
         |    max(vip_end_time),
         |    max(vip_last_modify_time),
         |    first(vip_max_free),
         |    first(vip_min_free),
         |    max(vip_next_level),
         |    first(vip_operator),dt,dn
         |FROM
         |(select
         |	a.uid,
         |	a.ad_id,
         |	a.fullname,
         |	a.iconurl,
         |	a.lastlogin,
         |	a.mailaddr,
         |	a.memberlevel,
         |	a.password,
         |	e.paymoney,
         |	a.phone,
         |	a.qq,
         |	a.register,
         |	a.regupdatetime,
         |	a.unitname,
         |	a.userip,
         |    a.zipcode,
         |    a.dt,
         |    b.appkey,
         |    b.appregurl,
         |    b.bdp_uuid,
         |    b.createtime as reg_createtime,
         |    b.domain,
         |    b.isranreg,
         |    b.regsource,
         |    b.regsourcename,
         |    c.adname,
         |    d.siteid,
         |    d.sitename,
         |    d.siteurl,
         |    d.`delete` as site_delete,
         |    d.createtime as site_createtime,
         |    d.creator as site_creator,
         |    f.vip_id,
         |    f.vip_level,
         |    f.start_time as vip_start_time,
         |    f.end_time as vip_end_time,
         |    f.last_modify_time as vip_last_modify_time,
         |    f.max_free as vip_max_free,
         |    f.min_free as vip_min_free,
         |    f.next_level as vip_next_level,f.operator as vip_operator,a.dn
         |    from
         |    dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid
         |    AND a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn
         |    left join dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn
         |    left join dwd.dwd_pcentermempaymoney e ON a.uid=e.uid and a.dn=e.dn
         |    left join dwd.dwd_vip_level f ON e.vip_id=f.vip_id and e.dn=f.dn where a.dt='$time') r
         | group by uid,dn,dt
       """.stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")
  }

  def importMemberZip(spark: SparkSession, time: String) = {
    import spark.implicits._
    val dayResult = spark.sql(
      s"""
         |select
         |a.uid,
         |sum(cast(a.paymoney as decimal(10,4))) as paymoney,
         |max(b.vip_level) as vip_level,
         |from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,
         |'9999-12-31' as end_time,
         |first(a.dn) as dn
         |from
         |dwd.dwd_pcentermempaymoney a join dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn
         |where a.dt='$time'
         |group by uid
       """.stripMargin).as[MemberZipper]

    val historyResult = spark.sql("select * from dws.dws_member_zipper").as[MemberZipper]

    val redult = dayResult.union(historyResult)
      .groupByKey(item => item.uid + "_" + item.dn)
      .mapGroups{
        case (key, iters) =>
          val keys = key.split("_")
          val uid = keys(0)
          val dn = keys(1)
          val list = iters.toList.sortBy(item => item.start_time)
          if (list.length > 1 && "9999-12-31".equals(list(list.length - 2).end_time) ) {
            val oldLastModel = list(list.length - 2)
            val lastModel = list(list.length - 1)

            oldLastModel.end_time = lastModel.start_time
//            lastModel.end_time = "9999-12-31"
            lastModel.paymoney = list.map(item => BigDecimal.apply(item.paymoney)).sum.toString
          }
        MemberZipperResult(list)
      }.flatMap(_.list).coalesce(2).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper")
  }

  def myTest(spark: SparkSession, time: String) = {
    spark.sql(
      s"""
         |select
         |	uid,
         |	first(ad_id),
         |	first(fullname),
         |	first(iconurl),
         |	first(lastlogin),
         |	first(mailaddr),
         |	first(memberlevel),
         |	first(password),
         |	sum(cast(paymoney as decimal(10,4))),
         |	first(phone),
         |	first(qq),
         |	first(register),
         |	first(regupdatetime),
         |	first(unitname),
         |	first(userip),
         |	first(zipcode),
         |    first(appkey),
         |    first(appregurl),
         |    first(bdp_uuid),
         |    first(reg_createtime),
         |    first(domain),
         |    first(isranreg),
         |    first(regsource),
         |    first(regsourcename),
         |    first(adname),
         |    first(siteid),
         |    first(sitename),
         |    first(siteurl),
         |    first(site_delete),
         |    first(site_createtime),
         |    first(site_creator),
         |    first(vip_id),
         |    max(vip_level),
         |    min(vip_start_time),
         |    max(vip_end_time),
         |    max(vip_last_modify_time),
         |    first(vip_max_free),
         |    first(vip_min_free),
         |    max(vip_next_level),
         |    first(vip_operator),dt,dn
         |FROM
         |(select
         |	a.uid,
         |	a.ad_id,
         |	a.fullname,
         |	a.iconurl,
         |	a.lastlogin,
         |	a.mailaddr,
         |	a.memberlevel,
         |	a.password,
         |	e.paymoney,
         |	a.phone,
         |	a.qq,
         |	a.register,
         |	a.regupdatetime,
         |	a.unitname,
         |	a.userip,
         |    a.zipcode,
         |    a.dt,
         |    b.appkey,
         |    b.appregurl,
         |    b.bdp_uuid,
         |    b.createtime as reg_createtime,
         |    b.domain,
         |    b.isranreg,
         |    b.regsource,
         |    b.regsourcename,
         |    c.adname,
         |    d.siteid,
         |    d.sitename,
         |    d.siteurl,
         |    d.`delete` as site_delete,
         |    d.createtime as site_createtime,
         |    d.creator as site_creator,
         |    f.vip_id,
         |    f.vip_level,
         |    f.start_time as vip_start_time,
         |    f.end_time as vip_end_time,
         |    f.last_modify_time as vip_last_modify_time,
         |    f.max_free as vip_max_free,
         |    f.min_free as vip_min_free,
         |    f.next_level as vip_next_level,f.operator as vip_operator,a.dn
         |    from
         |    dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid
         |    AND a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn
         |    left join dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn
         |    left join dwd.dwd_pcentermempaymoney e ON a.uid=e.uid and a.dn=e.dn
         |    left join dwd.dwd_vip_level f ON e.vip_id=f.vip_id and e.dn=f.dn where a.dt='$time') r
         | group by uid,dn,dt
       """.stripMargin)
  }
}
