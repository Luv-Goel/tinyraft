import asyncio
import argparse
from aiohttp import web
from tinyraft import Cluster, Client

async def handle_get(request):
    key = request.match_info.get('key')
    client = request.app['client']
    try:
        val = await client.get(key)
        if val is None:
            return web.Response(status=404, text="Key not found")
        return web.json_response({"key": key, "value": val})
    except Exception as e:
        return web.Response(status=500, text=str(e))

async def handle_set(request):
    key = request.match_info.get('key')
    client = request.app['client']
    try:
        data = await request.json()
        val = data.get('value')
        await client.set(key, val)
        return web.json_response({"status": "ok", "key": key, "value": val})
    except Exception as e:
        return web.Response(status=500, text=str(e))

async def handle_status(request):
    cluster = request.app['cluster']
    stats = []
    for node in cluster.nodes:
        stats.append(node.status())
    return web.json_response({"nodes": stats})

async def init_app(num_nodes=3):
    app = web.Application()
    
    cluster = Cluster(num_nodes)
    await cluster.start()
    
    print(f"Waiting for leader election among {num_nodes} nodes...")
    _, leader = await cluster.wait_for_leader()
    print(f"Leader elected: {leader.id}")
    
    client = Client({n.id: n for n in cluster.live()})
    
    app['cluster'] = cluster
    app['client'] = client
    
    app.router.add_get('/status', handle_status)
    app.router.add_get('/kv/{key}', handle_get)
    app.router.add_post('/kv/{key}', handle_set)
    
    return app

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="TinyRaft HTTP Proxy")
    parser.add_argument("--port", type=int, default=8080, help="HTTP port to listen on")
    parser.add_argument("--nodes", type=int, default=3, help="Number of raft nodes to spawn")
    args = parser.parse_args()
    
    web.run_app(init_app(args.nodes), port=args.port)
