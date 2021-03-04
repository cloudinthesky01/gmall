package com.project.bean

case class CouponAlertInfo(mid:String,
                          //领卷用户集合
                           uids:java.util.HashSet[String],
                          //事件为领取优惠卷时对应的商品集合
                           itemIds:java.util.HashSet[String],
                          //该设备上生成的事件集合
                           events:java.util.List[String],
                           ts:Long)
