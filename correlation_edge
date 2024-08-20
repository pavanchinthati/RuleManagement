import mgp
import json
from kafka import KafkaProducer
from datetime import datetime, timedelta
import uuid

logger = mgp.Logger()

KAFKA_TOPIC = "graphdb_correlation"
KAFKA_BROKERS = "10.0.10.60:9092,10.0.2.232:9092,10.0.3.105:9092,10.0.4.100:9092,10.0.15.102:9092"

def from_time(timestamp_str, time_to_subtract):
    # Parse the timestamp string into a datetime object
    # timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    try:
        # Try to parse with milliseconds
      timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        # Parse without milliseconds
      timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
    
    # Determine the unit and the amount of time to subtract
    if time_to_subtract.endswith('m'):  # minutes
        delta = timedelta(minutes=int(time_to_subtract[:-1]))
    elif time_to_subtract.endswith('h'):  # hours
        delta = timedelta(hours=int(time_to_subtract[:-1]))
    elif time_to_subtract.endswith('d'):  # days
        delta = timedelta(days=int(time_to_subtract[:-1]))
    else:
        raise ValueError("Invalid time format. Use 'm' for minutes, 'h' for hours, or 'd' for days.")
    
    # Subtract the time delta from the original timestamp
    new_timestamp = timestamp - delta
    
    # Return the new timestamp in the same format
    return new_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

# def from_time(timestamp_str, time_to_subtract):
#     # Define the format for the output timestamp
#     output_format = '%Y-%m-%dT%H:%M:%SZ'
    
#     # Try to parse the timestamp string into a datetime object
#     try:
#         # Try to parse with milliseconds
#         timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
#     except ValueError:
#         # Parse without milliseconds
#         timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
    
#     # Determine the unit and the amount of time to subtract
#     if time_to_subtract.endswith('m'):
#         amount = int(time_to_subtract.split()[0])
#         delta = timedelta(minutes=amount)
#     elif time_to_subtract.endswith('h'):
#         amount = int(time_to_subtract.split()[0])
#         delta = timedelta(hours=amount)
#     elif time_to_subtract.endswith('d'):
#         amount = int(time_to_subtract.split()[0])
#         delta = timedelta(days=amount)
#     else:
#         raise ValueError("Invalid time format. Use 'minutes', 'hours', or 'days'.")
    
    # Subtract the time delta from the original timestamp
    new_timestamp = timestamp - delta
    
    # Format the new timestamp in the desired format
    formatted_timestamp = new_timestamp.strftime(output_format)
    
    return formatted_timestamp

def send_to_kafka(edge_data):
    try:
        kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)
        kafka_producer.send(KAFKA_TOPIC, json.dumps(edge_data).encode("utf-8"))
        kafka_producer.flush()
        logger.info(f'Sent edge data to Kafka: {edge_data}')
    except Exception as e:
        logger.error(f'Error sending to Kafka: {e}')

@mgp.write_proc
def correlation_procedure(context: mgp.ProcCtx, correlation_edge: mgp.Any) -> mgp.Record(result=str):
    try:
        key = correlation_edge.get("key")
        # if key == "alert_id":
        #   latest_alert_id = correlation_edge.get("new")
        if key == "alert_created":
            edge = correlation_edge.get("edge")
            latest_timestamp = correlation_edge.get("new")
            latest_alert_id = edge.properties['alert_id']
            from_vertex = edge.from_vertex
            to_vertex = edge.to_vertex
            to_vertex_label = to_vertex.labels[0].name
            from_vertex_in_edges = from_vertex.in_edges
            correlations = []
            
            if to_vertex_label != 'Org':
                for in_edge in from_vertex_in_edges:
                    in_edge_from_vertex = in_edge.from_vertex
                    correlations.append(in_edge_from_vertex)
                    
                for corel in correlations:
                    groupby = corel.properties.get("group_by")
                    groupby_list = [
                        parts[0].lower() + parts[1].lower() if '.' in item else item.lower()
                        for item in groupby
                        for parts in [item.split('.')]
                    ]
                    timespan = corel.properties["timespan"]
                    org = corel.properties["org_id"]
                    corel_rule_id = corel.properties["id"]
                    corel_name = corel.properties["name"]
                    fromtime = from_time(latest_timestamp, timespan)
                    correlation_fired = False

                    rules = [edge.to_vertex for edge in corel.out_edges]
                    rules = [rule for rule in rules if rule != from_vertex]

                    alert_uuids_list = []
                    #alert_uuids_list.append(latest_alert_id)
                    
                    # for rule in rules:
                    #     rule_outedges = rule.out_edges
                    #     for edge in rule_outedges:
                    #        #logger.info(f"{fromtime}, {latest_timestamp}")
                    #       if edge.to_vertex==to_vertex:
                    #         alert_created = edge.properties["alert_created"]
                    #         if edge.to_vertex.labels[0].name.lower() in groupby_list:
                    #           # logger.info(f"{fromtime}, {latest_timestamp}")
                    #           if fromtime < alert_created:
                    #               alert_uuids_list.append(edge.properties["alert_id"])
                    #               correlation_fired = True
                    #               break
                                  
                    # if latest_alert_id not in alert_uuids_list:
                    #   alert_uuids_list.append(latest_alert_id)
        
                    # if not alert_uuids_list:
                    #     correlation_fired = False
                    #     break

                    # Initialize flag to track if all rules pass
                    all_rules_pass = True

                    # Loop through each rule
                    for rule in rules:
                        rule_outedges = rule.out_edges
                        rule_passed = False  # Flag to track if the current rule passes

                        # Loop through each edge of the rule
                        for edge in rule_outedges:
                            # Check if the edge's target vertex matches the to_vertex
                            if edge.to_vertex == to_vertex:
                                alert_created = edge.properties["alert_created"]
                                # Check if the vertex's label is in the groupby_list
                                if edge.to_vertex.labels[0].name.lower() in groupby_list:
                                    # Check if the alert was created after fromtime
                                    if fromtime < alert_created:
                                        alert_uuids_list.append(edge.properties["alert_id"])
                                        rule_passed = True  # Current rule passed
                                        break
                        
                        # If the current rule did not pass, set the flag to False and break the loop
                        if not rule_passed:
                            all_rules_pass = False
                            break

                    # After processing all rules
                    if all_rules_pass:
                        correlation_fired = True
                    else:
                        correlation_fired = False

                    # Ensure latest_alert_id is added if not already in alert_uuids_list
                    if latest_alert_id not in alert_uuids_list:
                        alert_uuids_list.append(latest_alert_id)


                    # logger.info(f"CORRELATION STATUS: {correlation_fired}")
                    
                    if correlation_fired:
                        correlation_uuid = str(uuid.uuid4())
                        edge_data = {
                            "uuid": correlation_uuid,
                            "alert": {
                                "created": latest_timestamp,
                                "kind": "alert",
                                "level": "medium"
                            },
                            "alert_uuid_list": list(alert_uuids_list),
                            "organization": {
                                "id": org
                            },
                            "rule": {
                                "description": "",
                                "id": corel_rule_id,
                                "name": corel_name
                            }
                        }
                        # Send the edge data to Kafka
                        # send_to_kafka(edge_data)
                        logger.info("***************** Messages has been sent to kafka *****************")
                        # logger.info(f"SEND TO KAFKA:- {edge_data}")
        # else:
        #     logger.info("no key")

        return mgp.Record(result="Edges data fetched and sent to Kafka successfully!")
    except Exception as err:
        logger.error("Error processing edge update: " + str(err))
        return mgp.Record(result="Error processing edge update")
