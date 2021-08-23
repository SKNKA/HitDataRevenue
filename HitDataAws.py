from pyspark.sql import SparkSession
from urllib.parse import urlparse,parse_qs
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import split, col, udf, when, lit , array, create_map, collect_list, trim,upper
from pyspark.sql import functions as F
import sys
import datetime

class hit_data:

    def hit_data_process_func(self):

        def url_search_engine_fun(url):
            if not url:
                return ''
            return urlparse(url).netloc

        def url_search_keyword_fun(url):
            parsed=urlparse(url)
            if parsed.query:
                # return parsed.query
                dic=parse_qs(parsed.query)
                if parsed.netloc =='search.yahoo.com':
                    return dic['p'][0].upper()
                else :
                    return dic['q'][0].upper()
            else:
                return ''

        url_search_engine_udf = udf(lambda m: url_search_engine_fun(m))
        url_search_keyword_udf = udf(lambda m: url_search_keyword_fun(m))

        spark=SparkSession.builder.appName("Intellij") \
            .getOrCreate()

        sc = spark.sparkContext
        sc.setLogLevel('ERROR')

        filepath = sys.argv[1]

        FileContent=spark.read \
            .option("header","true") \
            .option("sep", "\t") \
            .option("inferSchema", "true") \
            .csv(filepath)

        confirmed_purchase=FileContent.filter((FileContent.event_list.isin('1')) & (FileContent.pagename=='Order Complete'))

        referral_data=FileContent.filter(url_search_engine_udf(FileContent.page_url)!=url_search_engine_udf(FileContent.referrer))

        split_cols = split(confirmed_purchase.product_list, ';')

        confirmed_purchase_1=confirmed_purchase.withColumn("merchent_site",url_search_engine_udf(FileContent.referrer)) \
            .withColumn('sold_product',split_cols.getItem(1)) \
            .withColumn('price',split_cols.getItem(3).cast(IntegerType())) \
            .withColumn('SoldIPaddress',FileContent.ip)

        confirmed_purchase_2=confirmed_purchase_1.select("SoldIPaddress","pagename","merchent_site","sold_product","price")

        referral_data_1=referral_data.withColumn("Search_Engine_Domain",url_search_engine_udf(FileContent.referrer)) \
            .withColumn("Search_Keyword",url_search_keyword_udf(FileContent.referrer)) \
            .withColumn("SearchIPaddress",FileContent.ip)

        referral_data_2=referral_data_1.select("SearchIPaddress","pagename","Search_Engine_Domain","Search_Keyword")

        output_details=referral_data_2.join(confirmed_purchase_2,referral_data_2.SearchIPaddress ==  confirmed_purchase_2.SoldIPaddress,"inner")

        output_details_2=output_details.groupBy("Search_Engine_Domain","Search_Keyword").agg(F.sum("price").alias("Revenue")).orderBy(col("Revenue"),ascending=False)

        today_date = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')

        out_filename='s3n://bigdata-assessment/output/'+today_date

        output_details_2.coalesce(1) \
            .write \
            .option("sep", "\t") \
            .option("header","true") \
            .csv(out_filename)

hit_object=hit_data()
hit_object.hit_data_process_func()
