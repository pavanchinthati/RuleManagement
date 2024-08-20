import mgp
from datetime import datetime
import json
from kafka import KafkaProducer
from neo4j import GraphDatabase

logger = mgp.Logger()

# KAFKA_TOPIC = "alert_out2"
KAFKA_TOPIC = "graphdb_correlation"
KAFKA_BROKERS = "10.0.10.60:9092,10.0.2.232:9092,10.0.3.105:9092,10.0.4.100:9092,10.0.15.102:9092"

def send_to_kafka(payload):
    try:
        kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)
        kafka_producer.send(KAFKA_TOPIC, json.dumps(payload).encode("utf-8"))
        kafka_producer.flush()
        logger.info(f'Sent suppressed data to Kafka')
    except Exception as e:
        logger.error(f'Error sending to Kafka: {e}')

def get_entity_details(org_name, payload, rule):
    host = "bolt://localhost:7687"
    client = GraphDatabase.driver(
        host, auth=("", "")
    )

    query = f"""
        MATCH (org:Org {{id: $org_code}})<-[r:SUPPRESSED]-(rule:Rule {{id: $rule_id}}) 
        RETURN CASE 
            WHEN r IS NOT NULL THEN true 
            ELSE false 
        END AS relation_exists 
    """

    params = {
        'org_code': org_name,
        'rule_id': rule
    }
    result = client.execute_query(query, params)

    relation_exists = result.records[0]['relation_exists'] if result.records else False
    # logger.info(f"rel :: ---{relation_exists}")

    if relation_exists == False:
      logger.info(f"{rule} is suppressed on {org_name}")
      logger.info("sent non suppressed data to kafka")
    else:
      send_to_kafka(payload) 
      # logger.info(f"alert suppressed: {payload}")

@mgp.transformation
def suppress_transformation(context: mgp.TransCtx, messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    cypher_queries = []
    for i in range(messages.total_messages()):
        try:
            message = messages.message_at(i)
            data = message.payload().decode('utf-8')
            payload = json.loads(data)

            org_code = payload.get('organization', {}).get('id', "")
            rule_id = payload.get('rule', {}).get('id', "")
            # logger.info(f"data:------------>{org_code},{rule_id}")

            if (org_code != "" and rule_id != ""):
                # query_create_rels = """
                #     MERGE (org:Org {id: $org_code})
                #     MERGE (rule:Rule {id: $rule_id})
                #     MERGE (org)<-[:SUPPRESSED]-(rule)
                # """
                # parameters_rels = {"org_code": org_code, "rule_id": rule_id}
                # cypher_queries.append(mgp.Record(query=query_create_rels, parameters=parameters_rels))
                get_entity_details(org_name=org_code, payload=payload, rule=rule_id)
            else:
              pass
        except Exception as err:
            logger.error(f"ERROR OCCURRED : {err}")
    return []
