package com.privyalgo.util;

/**
 * 
 * @author scott
 * @date 2016-07-04
 */
public class AlgoConstants {

	public static final char ASCII1_SEPERATOR = '\001';

	public static final char ASCII2_SEPERATOR = '\002';
	
	public static final char ASCII3_SEPERATOR = '\003';

	public static final char COMMA_SEPARATOR = ',';

	public static final char BLANK_SEPERATOR = ' ';
	
	public static final char COLON_SEPERATOR = ':';

	public static final char TAB_SEPERATOR = '\t';

	public static final char SHARP_SEPERATOR = '#';
	
	public static final char UNDERLINE_SEPERATOR = '_';

	public static final char VERTICAL_BAR_SEPERATOR = '|';

	public static final String MIN_FEATURE_COUNT = "min.feature.count";

	public static final int DEFAULT_MIN_FEATURE_COUNT = 4;

	public static final int EXPECTED_INSERTIONS = 10000000;
	public static final double FALSE_POSITIVE_PROBABILITY = 0.0001d;
    
	public static final String APP_FEATURE_FILE_NAME = "app_feature";
	public static final String USER_BLOOMFILTER_FILE_NAME = "user_bloomfilter";
	
	public static final String CLIENT_ID = "1";

	public static final String TIME_ZONE = "12";
	
	public static final String DEVICE_LANG = "5";

	public static final String LONGITUDE = "1";

	public static final String LATITUDE = "2";
	
	public static final String CELL = "7";

	public static final String LOCATION_TIME = "8";

	public static final String MCCMNC =  "9";

	public static final String EVENT_TIME = "1";
	
	public static final String EVENT_NAME = "2";

	public static final int DEFAULT_TIME_ZONE = 480;

	public static final String GEOIP_FILE_NAME = "geoIp";
	
	public static final String MCC_FILE_NAME = "mcc";
	
	public static final String CITY_CONV_FILE_NAME = "cityConv";

	public static final String MCC_WEIGHT = "mcc.weight";
	
	public static final String IP_WEIGHT = "ip.weight";
	
	/** 判断用户是否是google用户的app */
	public static final String GOOGLE_USER_APP = "com.google.android.gms";
	
	/** 判断用户是否是facebook用户的app */
	public static final String FACEBOOK_USER_APP = "com.facebook.katana";
	
	/** 判断用户是否是twitter用户的app */
	public static final String TWITTER_USER_APP = "com.twitter.android";
	
	/** 用户不是google、facebook、twitter用户时，默认赋值为0 */
	public static final String USER_TYPE_NULL = "0";
	
	/** 有小孩的用户的标记 */
	public static final String IS_PARENT_LABEL = "1";
	
	/** 存放用来标记是否有小孩的用户的APP的文件名 */
	public static final String APP_MARK_FILENAME = "mark.list";
	public static final String APP_BLACK_FILENAME = "del.list";
	public static final String POSITIVE_FILENAME = "part-r-00000";
	public static final String SELECT_FEATURE_FILENAME = "part-00000";
    /** 用户活跃app输出结果目录名 */
	public final static String TOP = "Top";

	/** 用户活跃app输出用于下次增量计算的目录名 */
	public final static String ALL = "All";
	
	/** 用户是否有小孩特征文件 */
	public final static String PARENT_FEATURE = "parent_feature.dat";

	/** gameApp文件名称 **/
    public static final String GA_FILE_NAME = "ga.file.name";

    /** 默认gameApp文件名称 **/
    public static final String DEFAULT_GA_FILE_NAME = "gameApp.list";
    
    /** activeAppCode文件名称 **/
    public static final String AC_FILE_NAME = "ac.file.name";

    /** 默认activeAppCode文件名称 **/
    public static final String DEFAULT_AC_FILE_NAME = "appCode.txt";
    
	/** 美容白名单文件 */
	public static final String APP_BEAUTY_WHITE_FILE = "app.beauty.white.file";
	
	/** 美容用户缓存 */
	public static final String BEAUTY_USER_CACHE_FILE = "beauty.user";
	
	/** 美容活跃topApp */
	public static final String BEAUTY_USER_TOP_APP = "beauty.top.app";
	
	/** 用户性别白名单文件 **/
	public static final String FEMALE_APP = "female_app.dat";
	public static final String MALE_APP = "male_app.dat";
	
	/** 用户性别特征文件 **/
	public static final String APP_FEATURE = "app_feature.dat";
	       
	/** 用户性别特征长度文件标识 **/
	public static final String GENDER_FEATURE_SIZE_FILE = "FEATURE_SIZE_FILE";
	       
	/** 用户性别模型文件标识 **/
	public static final String GENDER_MODEL_FILE = "FEATURE_SIZE_FILE";
	       
	/** 用户性别预测的产品标识 launcher**/
	public static final String GENDER_PRODUCT_MARK_1 = "UAA";
	       
	/** 用户性别预测的产品标识 superb turboc**/
	public static final String GENDER_PRODUCT_MARK_2 = "SB";
       
	/** 用户爱好购物特征文件 **/
	public static final String SHOP_APP_FEATURE = "shop_app.dat";
}
