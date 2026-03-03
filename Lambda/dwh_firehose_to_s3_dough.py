import base64
import json
import logging
import os

# Configurar logging
log_level = os.environ.get('LOG_LEVEL', 'INFO')
logger = logging.getLogger()
logger.setLevel(log_level)


def handler(event, context):
    """
    Lambda function para procesar y filtrar datos de Kinesis Firehose.
    
    Esta función recibe registros de Firehose, los procesa/filtra y devuelve
    los registros transformados. Firehose espera una respuesta específica.
    
    Formato de entrada:
    {
        "invocationId": "string",
        "deliveryStreamArn": "string",
        "region": "string",
        "records": [
            {
                "recordId": "string",
                "approximateArrivalTimestamp": long,
                "data": "base64-encoded-string"
            }
        ]
    }
    
    Formato de salida:
    {
        "records": [
            {
                "recordId": "string",
                "result": "Ok" | "Dropped" | "ProcessingFailed",
                "data": "base64-encoded-string"
            }
        ]
    }
    """
    
    logger.info(f"Procesando {len(event['records'])} registros de Firehose")
    
    output_records = []
    
    for record in event['records']:
        record_id = record['recordId']
        
        try:
            # Decodificar el payload
            payload = base64.b64decode(record['data']).decode('utf-8')
            logger.debug(f"Payload decodificado: {payload}")
            
            # Intentar parsear como JSON
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                # Si no es JSON, tratarlo como texto plano
                data = payload
            
            # Filtrar registros que solo tienen metadata y no tienen datos reales
            if isinstance(data, dict):
                # Añadir un campo procesado
                data['processed_by'] = 'filter_control_files_lambda'
                data['lambda_version'] = context.function_version if context else 'local'

                # Filtrar registros de control basados en un campo
                if data.get('metadata', {}).get('is_control_file', False):
                    output_records.append({
                        'recordId': record_id,
                        'result': 'Dropped',
                        'data': record['data']
                    })
                    logger.info(f"Registro {record_id} descartado: archivo de control detectado")
                    continue

                # Filtrar registros que solo tienen metadata (no tienen otros campos además de metadata)
                if set(data.keys()) == {'metadata'}:
                    output_records.append({
                        'recordId': record_id,
                        'result': 'Dropped',
                        'data': record['data']
                    })
                    logger.info(f"Registro {record_id} descartado: solo metadata, sin datos reales")
                    continue
            
            # Re-codificar el registro procesado
            if isinstance(data, dict):
                processed_payload = json.dumps(data) + '\n'
            else:
                processed_payload = str(data)
            
            processed_data = base64.b64encode(processed_payload.encode('utf-8')).decode('utf-8')
            
            # Registro procesado exitosamente
            output_records.append({
                'recordId': record_id,
                'result': 'Ok',
                'data': processed_data
            })
            
            logger.debug(f"Registro {record_id} procesado exitosamente")
            
        except Exception as e:
            logger.error(f"Error procesando registro {record_id}: {str(e)}")
            # En caso de error, devolver el registro original como fallido
            output_records.append({
                'recordId': record_id,
                'result': 'ProcessingFailed',
                'data': record['data']
            })
    
    logger.info(f"Procesamiento completado. Total: {len(output_records)} registros")
    
    return {
        'records': output_records
    }