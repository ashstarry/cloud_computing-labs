import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    dynamodb = boto3.resource('dynamodb')
    client = boto3.client('dynamodb')
    table_city_distance = dynamodb.Table('city_distance')
    
    routes = event['graph'].split(',')
    city = set() 
    neighbor = {}
    dist_ret = {}
    for route in routes:
        ends = route.split('->')
        city.add(ends[0])
        city.add(ends[1])
        neighbor[ends[0]] = neighbor.get(ends[0], [])+ [ends[1]]
        #neighbor[ends[1]] = neighbor.get(ends[1], []).append(ends[0])
    city_num = len(city)
    for city_name_src in city:
        dist_ret[city_name_src] = {}
        for city_name_dest in city:
            dist_ret[city_name_src][city_name_dest] = 1000000
        #dist_ret[city_name_src][city_name_src] = 0
        visited = {}
        for city_name_dest in city:
            visited[city_name_dest] = False
        visited[city_name_src] = True
        queue = [city_name_src]
        step = 0
        while len(queue) > 0:
            cur = queue.pop(0)
            dist_ret[city_name_src][cur] = step
            if cur not in neighbor:
                continue
            for neighbor_city in neighbor[cur]:
                if visited[neighbor_city]:
                    continue
                visited[neighbor_city] = True
                queue.append(neighbor_city)
            step += 1    
    
    try:
        with table_city_distance.batch_writer() as batch:
            for dist_src in dist_ret:
                for dist_dest in dist_ret[dist_src]:
                    
                    if dist_ret[dist_src][dist_dest] == 1000000:
                        print('{}->{}:{}'.format(dist_src, dist_dest, -1))
                        batch.put_item(
                            Item={
                                'source': dist_src,
                                'destination': dist_dest,
                                'distance': -1
                            }
                        )
                    else:
                        print('{}->{}:{}'.format(dist_src, dist_dest, dist_ret[dist_src][dist_dest]))
                        batch.put_item(
                            Item={
                                'source': dist_src,
                                'destination': dist_dest,
                                'distance': dist_ret[dist_src][dist_dest]
                            }
                        )
        return {
            'statusCode': 200,
            'body': json.dumps('Succesful insert!')
        }
    except:
        print('Error')
        return {
                'statusCode': 400,
                'body': json.dumps('Error')
        }