import fluksoVisualizationWithGUI as fv
import os


def showSize(fsize):
    b = fsize
    kb = fsize * 0.001
    mb = fsize * 0.000001
    print("- {} Bytes = {} Kb = {} Mb".format(b, kb, mb))


def size(filename):
    fsize = os.path.getsize(filename)
    return fsize


def estimate(since):
    print("-- since : ", since)
    sensors, session, since_timing = fv.getFluksoData(since=since)
    home_ids = set(sensors["home_ID"])
    homes = fv.generateHomes(session, sensors, since, since_timing, home_ids)

    fv.saveFluksoData(homes.values())

    total_size = 0
    nb_tot_sensors = 0

    for hid, home in homes.items():
        print("==================================================================")
        nb_sensors = home.getNbFluksoSensors()
        print("Home {}, {} fluksos sensors : ".format(hid, nb_sensors))
        nb_tot_sensors += nb_sensors
        fsize = size("output/fluksoData/{}.csv".format(hid))
        showSize(fsize)
        total_size += fsize

    print("TOTAL SIZE ({} homes, {} sensors) : ".format(len(homes), nb_tot_sensors))
    showSize(total_size)


def main():
    filename = 'output/fluksoData/G1.csv'
    size(filename)

    # 1 flukso(fonctionnel):
    # - 20 min :
    estimate("1hours")
    # - 1 month:
    # estimate("30days")
    # - 1 year:
    # estimate("365days")
    # - all:
    # estimate("")

    # 17 maisons(tout) = 68 fluksos sensors:
    # - 1 an :
    # - tout :

    # mean over all houses :


if __name__ == "__main__":
    main()


