## This code is for creating organization, ruleid, hostname, username nodes and then creating relationships between them.


import mgp
import json

logger = mgp.Logger()

@mgp.transformation
def correlation_transformation(context: mgp.TransCtx, messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
  cypher_queries = []
  for i in range(messages.total_messages()):
    try:
      message = messages.message_at(i)
      data = message.payload().decode('utf-8')
      payload = json.loads(data)

      observables = payload.get('observables', {})
      org_code = payload.get('organization', {}).get('id', "")
      host_name = observables.get('host', {}).get('name', None)
      user_name = observables.get('user', {}).get('name',None)
      rule_id = payload.get('rule', {}).get('id',"")
      alert_created = payload.get('alert', {}).get('created',"")
      alert_id = payload.get('uuid', "") 

      if org_code is not None and (host_name is not None or user_name is not None) and rule_id is not None:
        # logger.info(f'ORGANIZATION: {org_code}')
        # logger.info(f'HOSTNAME: {host_name}')
        # logger.info(f'USERNAME: {user_name}')
        # logger.info(f'RULEID: {rule_id}')
        # logger.info(f'ALERTCREATED: {alert_created}')
        # logger.info(f'ALERTID: {alert_id}')

        # Second Step: Create Relationships and Set Properties
        query_create_rels = """
        // Match or create nodes
        MERGE (o:Org {id: $org_code})
        MERGE (h:Hostname {name: $host_name})
        MERGE (u:Username {name: $user_name})
        MERGE (r:Rule {id: $rule_id})

        // Match or create relationships
        MERGE (o)-[rel_host:HAS]->(h)
        MERGE (o)-[rel_user:HAS]->(u)
        MERGE (r)-[rel_rule:FIRED]->(o)
        MERGE (r)-[rel_host_rule:FIRED]->(h)
        MERGE (r)-[rel_user_rule:FIRED]->(u)

        // Update relationship properties
        SET
          // Update rel_rule properties
          rel_rule.count = COALESCE(rel_rule.count, 0) + 1,
          rel_rule.alert_created = $alert_created,
          rel_rule.alert_id = $alert_id,

          // Update rel_host_rule properties
          rel_host_rule.count = COALESCE(rel_host_rule.count, 0) + 1,
          rel_host_rule.alert_created = $alert_created,
          rel_host_rule.alert_id = $alert_id,


          // Update rel_user_rule properties
          rel_user_rule.count = COALESCE(rel_user_rule.count, 0) + 1,
          rel_user_rule.alert_created = $alert_created,
          rel_user_rule.alert_id = $alert_id
        """
        parameters_rels = {
            'org_code': org_code,
            'host_name': host_name,
            'user_name': user_name,
            'rule_id': rule_id,
            'alert_created': alert_created,
            'alert_id': alert_id
        }
        cypher_queries.append(mgp.Record(query=query_create_rels, parameters=parameters_rels))
        # logger.info("RELATIONSHIPS HAVE BEEN CREATED AND PROPERTIES SET.")
      # else:
      #   logger.info("NULL VALUES FOUND.")
      # else:
      #   logger.info("OBSERVABLES NOT FOUND.")
    except Exception as err:
        logger.error(f"ERROR OCCURRED: {err}")
  return cypher_queries

