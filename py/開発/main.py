from google.cloud import bigquery
import logging
import google.cloud.logging


# ログの設定
# 標準 Logger の設定
logging.basicConfig(
    format = "[%(asctime)s][%(levelname)s] %(message)s",
    level = logging.DEBUG
)
logger = logging.getLogger()

# Cloud Logging ハンドラを logger に接続
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()

# setup_logging() するとログレベルが INFO になるので DEBUG に変更
logger.setLevel(logging.DEBUG)
 

# URIを取得
def fetch_uri(data):
    bucket_name = data['bucket']
    file_name = data['name']
    uri = 'gs://{}/{}'.format(bucket_name, file_name)
    return uri


# 格納フォルダごとに該当するテーブルIDを取得
def fetch_table_id(data):
    file_name = data['name']
    table_id_mapping = {
        '1-1': 'dev_customer_visits',
        '1-2': 'dev_item_combinations',
        '1-3': 'dev_visit_date'
    }
    table_id = table_id_mapping.get(file_name.split('/')[0], None)
    return table_id


# 該当テーブルのスキーマ情報を取得
def fetch_table_schema(table_id):
    schema_mapping = {
        'dev_customer_visits': [
            bigquery.SchemaField('order_id', 'INTEGER'),
            bigquery.SchemaField('day', 'DATE'),
            bigquery.SchemaField('customer_id', 'INTEGER'),
            bigquery.SchemaField('item_name', 'STRING'),
            bigquery.SchemaField('price', 'INTEGER')
        ],
        'dev_item_combinations': [
            bigquery.SchemaField('day', 'DATE'),
            bigquery.SchemaField('customer_id', 'INTEGER'),
            bigquery.SchemaField('item', 'STRING')
        ],
        'dev_visit_date': [
            bigquery.SchemaField('date', 'DATE'),
            bigquery.SchemaField('oclock_8', 'INTEGER'),
            bigquery.SchemaField('oclock_9', 'INTEGER'),
            bigquery.SchemaField('oclock_10', 'INTEGER'),
            bigquery.SchemaField('oclock_11', 'INTEGER'),
            bigquery.SchemaField('oclock_12', 'INTEGER'),
            bigquery.SchemaField('oclock_13', 'INTEGER'),
            bigquery.SchemaField('oclock_14', 'INTEGER'),
            bigquery.SchemaField('oclock_15', 'INTEGER'),
            bigquery.SchemaField('oclock_16', 'INTEGER'),
            bigquery.SchemaField('oclock_17', 'INTEGER'),
            bigquery.SchemaField('oclock_18', 'INTEGER'),
            bigquery.SchemaField('oclock_19', 'INTEGER')
        ]
    }
    return schema_mapping.get(table_id)


# テーブルにレコードを追加
def insert_data_to_table(bq_instance, uri, dataset_id, table_id, table_schema):
    dataset_ref = bq_instance.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV

    job_config.schema = table_schema
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND  # テーブルにデータを追加

    # 追加ファイルのスキーマが定義通りの場合
    try:
        load_job = bq_instance.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()  # ロードジョブの完了を待機
        logger.info('Job finished.')
    # 追加ファイルのスキーマが誤っている場合
    except Exception as e:
        logger.error('Error loading data to table: {}'.format(e))


# メイン処理
def dev_load_data(data, context):
    
    # ファイルの型チェック
    if data['contentType'] != 'text/csv':
        logger.error('Not supported file type: %s', data['contentType'])
        return

    # ロード処理に必要な情報を変数に格納
    bq_instance = bigquery.Client()
    uri = fetch_uri(data)
    dataset_id = 'sales'
    table_id = fetch_table_id(data)
    table_schema = fetch_table_schema(table_id)
    dataset_id = 'sales'

    # ロード処理
    insert_data_to_table(bq_instance, uri, dataset_id, table_id, table_schema)
