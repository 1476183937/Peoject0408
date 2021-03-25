
ods层：

创建启动日志表ods_start_log
CREATE EXTERNAL TABLE ods_start_log (`line` string)
PARTITIONED BY (`dt` string)
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_start_log';

创建事件日志表ods_event_log
CREATE EXTERNAL TABLE ods_event_log(`line` string)
PARTITIONED BY (`dt` string)
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_event_log';

创建订单表
create external table ods_order_info (
    `id` string COMMENT '订单编号',
    `total_amount` decimal(10,2) COMMENT '订单金额',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id',
    `payment_way` string COMMENT '支付方式',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间'
) COMMENT '订单表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_order_info/';

创建订单详情表
create external table ods_order_detail( 
    `id` string COMMENT '订单编号',
    `order_id` string  COMMENT '订单号', 
    `user_id` string COMMENT '用户id',
    `sku_id` string COMMENT '商品id',
    `sku_name` string COMMENT '商品名称',
    `order_price` string COMMENT '商品价格',
    `sku_num` string COMMENT '商品数量',
    `create_time` string COMMENT '创建时间'
) COMMENT '订单明细表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t' 
location '/warehouse/gmall/ods/ods_order_detail/';

创建商品表
create external table ods_sku_info( 
    `id` string COMMENT 'skuId',
    `spu_id` string   COMMENT 'spuid', 
    `price` decimal(10,2) COMMENT '价格',
    `sku_name` string COMMENT '商品名称',
    `sku_desc` string COMMENT '商品描述',
    `weight` string COMMENT '重量',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `create_time` string COMMENT '创建时间'
) COMMENT '商品表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_sku_info/';

创建用户表
create external table ods_user_info( 
    `id` string COMMENT '用户id',
    `name`  string COMMENT '姓名',
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间'
) COMMENT '用户信息'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_user_info/';

创建商品一级分类表
create external table ods_base_category1( 
    `id` string COMMENT 'id',
    `name`  string COMMENT '名称'
) COMMENT '商品一级分类'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_base_category1/';

创建商品二级分类表
create external table ods_base_category2( 
    `id` string COMMENT ' id',
    `name` string COMMENT '名称',
    category1_id string COMMENT '一级品类id'
) COMMENT '商品二级分类'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_base_category2/';

创建商品三级分类表
create external table ods_base_category3(
    `id` string COMMENT ' id',
    `name`  string COMMENT '名称',
    category2_id string COMMENT '二级品类id'
) COMMENT '商品三级分类'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_base_category3/';

创建支付流水表
create external table ods_payment_info(
    `id`   bigint COMMENT '编号',
    `out_trade_no`    string COMMENT '对外业务编号',
    `order_id`        string COMMENT '订单编号',
    `user_id`         string COMMENT '用户编号',
    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
    `total_amount`    decimal(16,2) COMMENT '支付金额',
    `subject`         string COMMENT '交易内容',
    `payment_type`    string COMMENT '支付类型',
    `payment_time`    string COMMENT '支付时间'
   )  COMMENT '支付流水表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_payment_info/';



dwd层：

创建启动表
CREATE EXTERNAL TABLE dwd_start_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `entry` string, 
    `open_ad_type` string, 
    `action` string, 
    `loading_time` string, 
    `detail` string, 
    `extend1` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_start_log/';

创建基础明细表
CREATE EXTERNAL TABLE dwd_base_event_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string, 
    `app_time` string, 
    `network` string, 
    `lng` string, 
    `lat` string, 
    `event_name` string, 
    `event_json` string, 
    `server_time` string
    )
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_base_event_log/';


CREATE EXTERNAL TABLE dwd_display_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
    `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `action` string,
    `goodsid` string,
    `place` string,
    `extend1` string,
    `category` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_display_log/';

商品点击表
CREATE EXTERNAL TABLE dwd_display_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
    `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `action` string,
    `goodsid` string,
    `place` string,
    `extend1` string,
    `category` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_display_log/';

商品详情页表
CREATE EXTERNAL TABLE dwd_newsdetail_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string, 
    `app_time` string,  
    `network` string, 
    `lng` string, 
    `lat` string, 
    `entry` string,
    `action` string,
    `goodsid` string,
    `showtype` string,
    `news_staytime` string,
    `loading_time` string,
    `type1` string,
    `category` string,
    `server_time` string)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_newsdetail_log/';

商品列表页表
CREATE EXTERNAL TABLE dwd_loading_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `action` string,
    `loading_time` string,
    `loading_way` string,
    `extend1` string,
    `extend2` string,
    `type` string,
    `type1` string,
    `server_time` string
    )
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_loading_log/';

广告表
CREATE EXTERNAL TABLE dwd_ad_log(
    mid_id` string,
    user_id` string, 
    version_code` string, 
    version_name` string, 
    lang` string, 
    source` string, 
    os` string, 
    area` string, 
    model` string,
    brand` string, 
    sdk_version` string, 
    gmail` string, 
    height_width` string,  
    app_time` string,
    network` string, 
    lng` string, 
    lat` string, 
    entry` string,
    action` string,
    content` string,
    detail` string,
    ad_source` string,
    behavior` string,
    newstype` string,
    show_style` string,
    server_time` string
    )
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_ad_log/';

消息通知表
CREATE EXTERNAL TABLE dwd_notification_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string,
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `action` string,
    `noti_type` string,
    `ap_time` string,
    `content` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_notification_log/';

用户前台活跃表
CREATE EXTERNAL TABLE dwd_active_foreground_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
    `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `push_id` string,
    `access` string,
    `server_time` string)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_foreground_log/';

用户后台活跃表
CREATE EXTERNAL TABLE dwd_active_background_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
     `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `active_source` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_background_log/';

评论表
CREATE EXTERNAL TABLE dwd_comment_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
    `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `comment_id` int,
    `userid` int,
    `p_comment_id` int, 
    `content` string,
    `addtime` string,
    `other_id` int,
    `praise_count` int,
    `reply_count` int,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_comment_log/';

收藏表
CREATE EXTERNAL TABLE dwd_favorites_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `id` int, 
    `course_id` int, 
    `userid` int,
    `add_time` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_favorites_log/';

点赞表
CREATE EXTERNAL TABLE dwd_praise_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `id` string, 
    `userid` string, 
    `target_id` string,
    `type` string,
    `add_time` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_praise_log/';


错误日志表
CREATE EXTERNAL TABLE dwd_error_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `errorBrief` string, 
    `errorDetail` string, 
    `server_time` string
    )
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_error_log/';

创建订单表
create external table dwd_order_info (
    `id` string COMMENT '',
    `total_amount` decimal(10,2) COMMENT '',
    `order_status` string COMMENT ' 1 2 3 4 5',
    `user_id` string COMMENT 'id',
    `payment_way` string COMMENT '',
    `out_trade_no` string COMMENT '',
    `create_time` string COMMENT '',
    `operate_time` string COMMENT ''
) 
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_order_info/'
tblproperties ("parquet.compression"="snappy");

创建订单详情表
create external table dwd_order_detail( 
    `id` string COMMENT '',
    `order_id` decimal(10,2) COMMENT '', 
    `user_id` string COMMENT 'id',
    `sku_id` string COMMENT 'id',
    `sku_name` string COMMENT '',
    `order_price` string COMMENT '',
    `sku_num` string COMMENT '',
    `create_time` string COMMENT ''
)
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_order_detail/'
tblproperties ("parquet.compression"="snappy");

创建用户表
create external table dwd_user_info( 
    `id` string COMMENT 'id',
    `name` string COMMENT '', 
    `birthday` string COMMENT '',
    `gender` string COMMENT '',
    `email` string COMMENT '',
    `user_level` string COMMENT '',
    `create_time` string COMMENT ''
) 
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_user_info/'
tblproperties ("parquet.compression"="snappy");

创建支付流水表
create external table dwd_payment_info(
    `id`   bigint COMMENT '',
    `out_trade_no`    string COMMENT '',
    `order_id`        string COMMENT '',
    `user_id`         string COMMENT '',
    `alipay_trade_no` string COMMENT '',
    `total_amount`    decimal(16,2) COMMENT '',
    `subject`         string COMMENT '',
    `payment_type`    string COMMENT '',
    `payment_time`    string COMMENT ''
   )  
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_payment_info/'
tblproperties ("parquet.compression"="snappy");

创建商品表（增加分类）
create external table dwd_sku_info(
    `id` string COMMENT 'skuId',
    `spu_id` string COMMENT 'spuid',
    `price` decimal(10,2) COMMENT '',
    `sku_name` string COMMENT '',
    `sku_desc` string COMMENT '',
    `weight` string COMMENT '',
    `tm_id` string COMMENT 'id',
    `category3_id` string COMMENT '1id',
    `category2_id` string COMMENT '2id',
    `category1_id` string COMMENT '3id',
    `category3_name` string COMMENT '3',
    `category2_name` string COMMENT '2',
    `category1_name` string COMMENT '1',
    `create_time` string COMMENT ''
) 
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="snappy");



dws层：

每日活跃设备明细
create external table dws_uv_detail_day
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度'
)
partitioned by(dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_day';

每周活跃设备明细
create external table dws_uv_detail_wk( 
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `monday_date` string COMMENT '周一日期',
    `sunday_date` string COMMENT  '周日日期' 
) COMMENT '活跃用户按周明细'
PARTITIONED BY (`wk_dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_wk/';

每月活跃设备明细
create external table dws_uv_detail_mn( 
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度'
) COMMENT '活跃用户按月明细'
PARTITIONED BY (`mn` string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_mn/';

每日新增设备明细表
create external table dws_new_mid_day
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `create_date`  string  comment '创建时间' 
)  COMMENT '每日新增设备信息'
stored as parquet
location '/warehouse/gmall/dws/dws_new_mid_day/';

每日留存用户明细表
create external table dws_user_retention_day 
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `create_date`    string  comment '设备新增时间',
    `retention_day`  int comment '截止当前日期留存天数'
)  COMMENT '每日用户留存情况'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_retention_day/';


创建用户行为宽表
create external table dws_user_action 
(   
    user_id          string      comment '用户 id',
    order_count     bigint      comment '下单次数 ',
    order_amount    decimal(16,2)  comment '下单金额 ',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额 ',
    comment_count   bigint      comment '评论次数'
) COMMENT '每日用户行为宽表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_action/'
tblproperties ("parquet.compression"="snappy");

用户购买商品明细表（宽表）
create external table dws_sale_detail_daycount
(
    user_id   string  comment '用户 id',
    sku_id    string comment '商品 Id',
    user_gender  string comment '用户性别',
    user_age string  comment '用户年龄',
    user_level string comment '用户等级',
    order_price decimal(10,2) comment '商品价格',
    sku_name string   comment '商品名称',
    sku_tm_id string   comment '品牌id',
    sku_category3_id string comment '商品三级品类id',
    sku_category2_id string comment '商品二级品类id',
    sku_category1_id string comment '商品一级品类id',
    sku_category3_name string comment '商品三级品类名称',
    sku_category2_name string comment '商品二级品类名称',
    sku_category1_name string comment '商品一级品类名称',
    spu_id  string comment '商品 spu',
    sku_num  int comment '购买个数',
    order_count string comment '当日下单单数',
    order_amount string comment '当日下单金额'
) COMMENT '用户购买商品明细表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_sale_detail_daycount/'
tblproperties ("parquet.compression"="snappy");


ads层：
活跃设备数
create external table ads_uv_count( 
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日用户数量',
    `wk_count`  bigint COMMENT '当周用户数量',
    `mn_count`  bigint COMMENT '当月用户数量',
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果' 
) COMMENT '活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/';


每日新增设备表
create external table ads_new_mid_count
(
    `create_date`     string comment '创建时间' ,
    `new_mid_count`   BIGINT comment '新增设备数量' 
)  COMMENT '每日新增设备信息数量'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';

留存用户数
create external table ads_user_retention_day_count 
(
    `create_date`       string  comment '设备新增日期',
    `retention_day`     int comment '截止当前日期留存天数',
    `retention_count`    bigint comment  '留存数量'
)  COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_count/';

沉默用户：指的是只在安装当天启动过，且启动时间是在一周前
create external table ads_slient_count( 
    `dt` string COMMENT '统计日期',
    `slient_count` bigint COMMENT '沉默设备数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_slient_count';

本周回流=本周活跃-本周新增-上周活跃
create external table ads_back_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_back_count';

流失用户：最近7天未登录我们称之为流失用户
create external table ads_wastage_count( 
    `dt` string COMMENT '统计日期',
    `wastage_count` bigint COMMENT '流失设备数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_wastage_count';

最近3周连续活跃的用户：通常是周一对前3周的数据做统计，该数据一周计算一次。
create external table ads_continuity_wk_count( 
    `dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
    `wk_dt` string COMMENT '持续时间',
    `continuity_count` bigint
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_wk_count';

最近七天内连续三天活跃用户数
create external table ads_continuity_uv_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '最近7天日期',
    `continuity_count` bigint
) COMMENT '连续活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_uv_count';


GMV成交总额
create external table ads_gmv_sum_day(
    `dt` string COMMENT '统计日期',
    `gmv_count`  bigint COMMENT '当日gmv订单个数',
    `gmv_amount`  decimal(16,2) COMMENT '当日gmv订单总金额',
    `gmv_payment`  decimal(16,2) COMMENT '当日支付金额'
) COMMENT 'GMV'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_gmv_sum_day/';

转化率及漏斗分析
新增用户占日活跃用户比率
create external table ads_user_convert_day( 
    `dt` string COMMENT '统计日期',
    `uv_m_count`  bigint COMMENT '当日活跃设备',
    `new_m_count`  bigint COMMENT '当日新增设备',
    `new_m_ratio`   decimal(10,2) COMMENT '当日新增占日活的比率'
) COMMENT '转化率'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_convert_day/';

用户行为漏斗分析
create external  table ads_user_action_convert_day(
    `dt` string COMMENT '统计日期',
    `total_visitor_m_count`  bigint COMMENT '总访问人数',
    `order_u_count` bigint     COMMENT '下单人数',
    `visitor2order_convert_ratio`  decimal(10,2) COMMENT '访问到下单转化率',
    `payment_u_count` bigint     COMMENT '支付人数',
    `order2payment_convert_ratio` decimal(10,2) COMMENT '下单到支付的转化率'
 ) COMMENT '用户行为漏斗分析'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_action_convert_day/';

品牌复购率
create external table ads_sale_tm_category1_stat_mn
(   
    tm_id string comment '品牌id',
    category1_id string comment '1级品类id ',
    category1_name string comment '1级品类名称 ',
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(10,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(10,2)  comment  '多次复购率',
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期' 
)   COMMENT '复购率统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';

