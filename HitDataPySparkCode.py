from pyspark.sql import SparkSession
from urllib.parse import urlparse,parse_qs
from pyspark.sql.functions import split, col, udf, when, lit , array, create_map, collect_list, trim,upper
from pyspark.sql import functions as F
import sys,datetime
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, ArrayType, MapType

class hit_data:

    def hit_data_process_func(self):
        #udf function to get the search engine
        def url_search_engine_fun(url):
            if not url:
                return ''
            return urlparse(url).netloc

        #udf function to get the search keyword
        def url_search_keyword_fun(url):
            parsed=urlparse(url)
            if parsed.query:
                dic=parse_qs(parsed.query)
                if parsed.netloc =='search.yahoo.com':
                    return dic['p'][0].upper()
                else :
                    return dic['q'][0].upper()
            else:
                return ''

        url_search_engine_udf = udf(lambda m: url_search_engine_fun(m))
        url_search_keyword_udf = udf(lambda m: url_search_keyword_fun(m))

        #Defining schema
        schema = StructType([
            StructField("hit_time_gmt",StringType(),True),
            StructField("date_time",StringType(),True),
            StructField("user_agent",StringType(),True),
            StructField("ip",StringType(),True),
            StructField("event_list",StringType(),True),
            StructField("geo_city",StringType(),True),
            StructField("geo_region",StringType(),True),
            StructField("geo_country", StringType(), True),
            StructField("pagename", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("product_list", StringType(), True),
            StructField("referrer", StringType(), True)
        ])

        #setting Spark session
        spark=SparkSession.builder.appName("Intellij") \
            .getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel('ERROR')

        # reading the input argument submitted during spark-submit
        filepath = sys.argv[1]

        # read file content into DF
        FileContent=spark.read \
            .option("header","true") \
            .option("sep", "\t") \
            .schema(schema)\
            .csv(filepath)

        # Filter confirmed purchases into DF
        confirmed_purchase=FileContent.filter((FileContent.event_list.isin('1')) & (FileContent.pagename=='Order Complete'))

        # Filter referral data into DF
        referral_data=FileContent.filter(url_search_engine_udf(FileContent.page_url)!=url_search_engine_udf(FileContent.referrer))

        # splitting and renaming required columns for confirmed purchases
        confirmed_purchase_1=confirmed_purchase.withColumn("merchent_site",url_search_engine_udf(confirmed_purchase.referrer)) \
            .withColumn('sold_product',split(confirmed_purchase.product_list, ';').getItem(1)) \
            .withColumn('price',split(confirmed_purchase.product_list, ';').getItem(3).cast(IntegerType())) \
            .withColumn('SoldIPaddress',confirmed_purchase.ip)

        # selecting required columns
        confirmed_purchase_2=confirmed_purchase_1.select("SoldIPaddress","pagename","merchent_site","sold_product","price")

        # splitting and renaming required columns for referral purchases
        referral_data_1=referral_data.withColumn("Search_Engine_Domain",url_search_engine_udf(referral_data.referrer)) \
            .withColumn("Search_Keyword",url_search_keyword_udf(referral_data.referrer)) \
            .withColumn("SearchIPaddress",referral_data.ip)

        # selecting required columns
        referral_data_2=referral_data_1.select("SearchIPaddress","Search_Engine_Domain","Search_Keyword")

        # join referral and confirmed purchases to ge the combined results
        output_details=referral_data_2.join(confirmed_purchase_2,referral_data_2.SearchIPaddress ==  confirmed_purchase_2.SoldIPaddress,"inner")

        # aggregate the results based on search engine and key word
        output_details_2=output_details.groupBy("Search_Engine_Domain","Search_Keyword").agg(F.sum("price").alias("Revenue")).orderBy(col("Revenue"),ascending=False)

        # creating folder details for Aws S3
        today_date = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
        out_filename='s3n://bigdata-assessment/output/'+today_date

        # writing all the details into single file
        output_details_2.coalesce(1) \
            .write \
            .option("sep", "\t") \
            .option("header","true") \
            .csv(out_filename)

# calling objects.
hit_object=hit_data()
hit_object.hit_data_process_func()
