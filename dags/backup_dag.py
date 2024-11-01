from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from utils.common import *

def perform_backup(**context):
    try:
        engine = get_db_engine()
        
        # Requête optimisée utilisant les index
        query_template = """
        SELECT 
            transaction_id,
            cc_num,
            merchant,
            category,
            amt,
            first,
            last,
            gender,
            street,
            city,
            state,
            zip,
            lat,
            long,
            city_pop,
            job,
            dob,
            trans_num,
            merch_lat,
            merch_long,
            is_fraud,
            trans_date_trans_time
        FROM {table}
        WHERE trans_date_trans_time >= NOW() - INTERVAL '30 days'
        ORDER BY trans_date_trans_time DESC  -- Utilise l'index pour le tri
        """
        
        # Récupération avec monitoring du temps
        start_time = datetime.now()
        
        # Récupération des données avec statistiques d'exécution
        with engine.connect() as conn:
            # Analyse des requêtes
            conn.execute("ANALYZE fraud_transactions")
            conn.execute("ANALYZE normal_transactions")
            
            logger.info("Récupération des transactions frauduleuses...")
            fraud_df = pd.read_sql(query_template.format(table='fraud_transactions'), conn)
            
            logger.info("Récupération des transactions normales...")
            normal_df = pd.read_sql(query_template.format(table='normal_transactions'), conn)
        
        query_time = (datetime.now() - start_time).total_seconds()
        
        # Combinaison des données
        combined_df = pd.concat([fraud_df, normal_df], ignore_index=True)
        
        if combined_df.empty:
            logger.warning("Pas de données à sauvegarder pour les 30 derniers jours")
            return
            
        # Stats pour le rapport
        stats = {
            'total_transactions': len(combined_df),
            'fraud_transactions': len(fraud_df),
            'normal_transactions': len(normal_df),
            'total_amount': combined_df['amt'].sum(),
            'period_start': combined_df['trans_date_trans_time'].min(),
            'period_end': combined_df['trans_date_trans_time'].max(),
            'query_time': query_time
        }
        
        # Sauvegarde temporaire
        tmp_file = '/tmp/monthly_backup.csv'
        combined_df.to_csv(tmp_file, index=False)
        
        # Upload vers S3
        current_date = datetime.now()
        s3_key = f"{S3_PREFIX}/backups/monthly_{current_date.strftime('%Y%m')}.csv"
        
        start_upload = datetime.now()
        s3_client = get_s3_client()
        s3_client.upload_file(tmp_file, S3_BUCKET, s3_key)
        upload_time = (datetime.now() - start_upload).total_seconds()
        
        # Nettoyage
        os.remove(tmp_file)
        
        # Notification avec performances
        email_body = f"""
        Backup mensuel effectué avec succès!

        Statistiques du backup :
        - Période : du {stats['period_start']} au {stats['period_end']}
        - Total transactions : {stats['total_transactions']:,}
        - Transactions normales : {stats['normal_transactions']:,}
        - Transactions frauduleuses : {stats['fraud_transactions']:,}
        - Montant total : {stats['total_amount']:,.2f}€

        Performances :
        - Temps d'extraction : {stats['query_time']:.2f} secondes
        - Temps d'upload S3 : {upload_time:.2f} secondes
        - Taille du fichier : {os.path.getsize(tmp_file) / (1024*1024):.2f} MB

        Fichier sauvegardé : s3://{S3_BUCKET}/{s3_key}
        """
        
        send_email(
            subject=f"Backup mensuel des transactions - {current_date.strftime('%B %Y')}",
            body=email_body
        )
        
        logger.info(f"Backup terminé : {stats['total_transactions']} transactions sauvegardées")
        
    except Exception as e:
        logger.error(f"Erreur de backup: {str(e)}")
        raise

default_args = {
    'owner': 'fraud_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'fraud_backup',
    default_args=default_args,
    description='Backup mensuel des données de transactions',
    schedule_interval='0 0 1 * *',  # Tous les 1er du mois à minuit
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    backup = PythonOperator(
        task_id='perform_backup',
        python_callable=perform_backup,
        provide_context=True
    )