import fluksoVisualizationWithGUI as fv
import os
import json


def showSize(fsize):
    b = fsize
    kb = fsize * 0.001
    mb = fsize * 0.000001
    gb = fsize * 0.000000001
    print("- {} Bytes = {} Kb = {} Mb = {} Gb".format(b, kb, mb, gb))
    return {"b": b, "kb": kb, "mb": mb, "gb": gb}


def size(filename):
    fsize = os.path.getsize(filename)
    return fsize


def toJson(sizes, filename):
    print(filename)
    with open(filename, 'w') as f:
        json.dump(sizes, f)


def estimate(since):
    print("-- since : ", since)
    sensors, session, since_timing = fv.getFluksoData(since=since)
    home_ids = set(sensors["home_ID"])
    homes = fv.generateHomes(session, sensors, since, since_timing, home_ids)

    fv.saveFluksoData(homes.values())

    total_size = 0
    nb_tot_sensors = 0

    sizes = {}

    for hid, home in homes.items():
        nb_sensors = home.getNbFluksoSensors()
        print("Home {}, {} fluksos sensors : ".format(hid, nb_sensors))
        nb_tot_sensors += nb_sensors
        fsize = size("output/fluksoData/{}.csv".format(hid))
        size_dic = showSize(fsize)
        total_size += fsize
        sizes[hid] = size_dic

    print("TOTAL SIZE ({} homes, {} sensors) : ".format(len(homes), nb_tot_sensors))
    tot_size_dic = showSize(total_size)
    sizes["Total"] = tot_size_dic

    print(sizes)

    toJson(sizes, "output/fluksoData/sizes_{}.json".format(since))

    return sizes


def main():
    filename = 'output/fluksoData/G1.csv'
    size(filename)

    # 1 flukso(fonctionnel):
    # - 1 hour:
    sizes_1h = estimate("1hours")
    # - 1 day :
    sizes_1d = estimate("1days")
    # - 1 month:
    sizes_1m = estimate("30days")
    # - 1 year:
    sizes_1y = estimate("365days")
    # - all:
    # estimate("")

    # 17 maisons(tout) = 68 fluksos sensors:
    # - 1 an :
    # - tout :

    # mean over all houses :


if __name__ == "__main__":
    main()


