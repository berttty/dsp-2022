import overpy
import sys as s
if __name__ == '__main__':

    print(s.modules.keys())
    print(overpy)
    # way(52.511377,13.341624,52.511920,13.342587);
    #way(50.746,7.154,50.748,7.157) ["highway"];way(52.508303,13.337725,52.508336,13.339774);
    api = overpy.Overpass()

    # fetch all ways and nodes
    result = api.query("""[out:json];
        way(52.50657262180007, 13.331419632311839,52.50806782318529, 13.333171853792031) ["highway"];
        (._;>;);
        out body;
        """)

    # result = api.query("""way(52.50657262180007, 13.331419632311839,52.50806782318529, 13.333171853792031);(._;>;);out body;""")

    # print(result.way.tags.get("surface", "n/a"))
    for way in result.ways:
        surface = way.tags.get("surface");
        if surface != "":
            for node in way.nodes:
                print("    Lat: %f, Lon: %f, Surface: %s" % (node.lat, node.lon, surface))

        # print("Name: %s" % way.tags.get("name", "n/a"))
        # print("todo: ", way.tags)
        # print("  natural: %s" % way.tags.get("natural", "n/a"))
        # print("  Nodes:")
        # for node in way.nodes:
        #     print("    Lat: %f, Lon: %f" % (node.lat, node.lon))
