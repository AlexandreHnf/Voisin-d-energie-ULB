const domain_name = 'http://localhost:5000';

var timing_button = document.getElementById("buttonShow"); 

let charts_rtu = {today : null, day : null};
let rtu_powers_data = {};
let current_date = new Date().toISOString().slice(0, 10).toString();

let COLORS = ["#34ace0", "#e55039", "#474787", "#fed330", "#32ff7e", "#0fb9b1", "#fa8231", "#a5b1c2",
              "#4b7bec", "#a55eea", "#fad390", "#b71540", "#e58e26", "#38ada9", "#0a3d62"];

const VERBOSE = true;

function activateLoader() {
	/*
	make the loader appear and spin
	*/
	document.getElementById("loader").style.display = "block";
}

function deactivateLoader() {
	/* 
	make the loader animation disappear after a certain amount of time
  -> it simulates the actual loading time of the data arriving in the graphs.
	*/
	setTimeout(() =>{
		document.getElementById("loader").style.display = "none";
	}, 3000);  // 3 seconds
}

function downloadRTU() {
    /* 
    // Download flukso data of the home and of the groups
    */
    // convert to csv 
    let csv = $.csv.fromObjects(rtu_powers_data);

    // trigger a download
    var downloadLink = document.createElement("a");
    var blob = new Blob([csv], { type: 'text/csv' });
    var url = URL.createObjectURL(blob);
    downloadLink.href = url;
    downloadLink.download = `data_RTU_${current_date}.csv`;
    document.body.appendChild(downloadLink);
    downloadLink.click();
    document.body.removeChild(downloadLink);
}

function getEnergy(powers_data, tot_power) {
    /*
    from the total power value, compute the total energy
    given a certain number of data points and timestamps
    Energy : volume under the curve
    */

    let N = powers_data.length;  // number of data points
    let T = 0                                       // number of hours 
    if (N > 0) {
        first_ts = new Date(powers_data[0].ts.slice(0, -5))
        last_ts = new Date(powers_data[N-1].ts.slice(0, -5))
        T = Math.abs(last_ts - first_ts) / 36e5;
    }
    // first compute the mean :
    let P_mean = Math.abs(tot_power) / N;
    Q_tot = (P_mean / 1000) * T;  // divided by 1000 to convert to KWh

    if (isNaN(Q_tot)) {
        Q_tot = 0;
    }
    Q_tot = Math.round(Q_tot);
    console.log(Q_tot);

    return Q_tot;
}

function updateStatsTableRTU(powers_data, totals, table_name) {
    document.getElementById(table_name).rows[1].cells[0].innerHTML = getEnergy(powers_data, totals.p_active_tot);
    document.getElementById(table_name).rows[1].cells[1].innerHTML = getEnergy(powers_data, totals.p_pre_tot);
}

function createChartPowersRTU(powers_data, date) {
    /* 
    fill chart with received data 
    */
    initChartRTU(charts_rtu);
  
    data = []
    totals = {p_active_tot: 0, p_pre_tot: 0}
    for (let i = 0; i < powers_data.length; i++) {
        let row = powers_data[i];

        let tss = new Date(row.ts);
        let ts = date + "T" + tss.toTimeString().slice(0,8) // local timezone (CET)

        charts_rtu.day.data.datasets[0].data.push({x: ts, p_active: row["active"]});
        charts_rtu.day.data.datasets[1].data.push({x: ts, pre: row["active"]});

        totals.p_active_tot += row["active"];
        totals.p_pre_tot += row["active"];
    }
    charts_rtu.day.update();
    deactivateLoader();
    updateStatsTableRTU(powers_data, totals, `stat_table_RTU`);
}

function processDateQueryRTU() {
	/* 
	Get a date input, send query to server
	*/
	var date = document.getElementById("day").value;
	let date_badge = document.querySelectorAll('[id=date_badge]');
	date_badge.forEach(badge => {
		badge.innerHTML = date;
	})
	activateLoader();
    sendDateQueryRTU("rtu", date.toString());
}

async function sendDateQueryRTU(data_type, date) {
    /* 
    send pseudo to server
    */
    const data = { date: date, data_type: data_type };
    const options = {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
    };

    const response = await fetch('/date_rtu', options);
    const resdata = await response.json();

    var alldata = resdata.data;
    if (alldata != undefined) {
        if (VERBOSE) {console.log("nb of rows received : " + alldata.length);}

        if (alldata.length == 0) {
            document.getElementById(`query_msg_${data_type}`).innerHTML = "<strong>" + date + "</strong> : Aucune donnée."
        } else {
            document.getElementById(`query_msg_${data_type}`).innerHTML = "";
        }
        // if (VERBOSE) {console.log(alldata[0]);}

        rtu_powers_data = alldata;
        current_date = date;

        createChartPowersRTU(alldata, date);
    }
}

function initFirstQueryRTU() {
    /* 
    When arriving in the main page, init a first query to get today's data
    */
    // set active tab : power
    document.getElementById("power_rtu_tab").click();  // simulates a click

    // query data of today
    let today = new Date().toISOString().slice(0, 10);  // format (YYYY MM DD)
    let date_badge = document.querySelectorAll('[id=date_badge]');
    date_badge.forEach(badge => {
        badge.innerHTML = today;
    })
    activateLoader();
    sendDateQueryRTU("rtu", today.toString());
}


function createChartCanvasRTU(col_name) {
    /* 
    create html chart canvas :
    may be cleaner if the html were not defined here. To improve
    */
    let div = document.createElement("div");
    div.innerHTML += `<b>Cabine basse tension</b><br>
                                            <p id="captionRTU"> </p><br>
                                            <canvas id="chartCanvas_RTU"></canvas>
                                            <div class="alert alert-secondary" role="alert">
                                            <!-- Stats table with total injection and total taking (Prelevement) -->
                                            <h3>Energies</h3>
                                            <p>Energies totales de la journée</p>
                                            <table id="stat_table_RTU" class="table table-striped">
                                                <thead>
                                                    <tr>
                                                        <th>Consommation (kWh)</th>
                                                        <th>Prelevement - P_tot > 0 (kWh)</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <td></td>
                                                        <td></td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                        </div>
                                        `;
            
    document.getElementById(`${col_name}`).appendChild(div);
}

function createChartDatasetpowersRTU() {
    /* 
    create Chartjs dataset for power data
    labels : P_cons
    */
    datasets = [
        {
            label: 'P_active',
            data: [],
            parsing: {
                yAxisKey: 'p_active'
            },
            borderColor: 'rgb(255, 99, 132)',
            backgroundColor: Samples.utils.transparentize(255, 99, 132, 0.4),
            borderWidth: 1,
            pointRadius: 1.2,
            hidden: false,
            fill: false
        },
        {
            label: 'Prélèvement',
            data: [],
            parsing: {
                yAxisKey: 'pre'
            },
            borderColor: window.chartColors.yellow,
            borderWidth: 1, 
            pointRadius: 0,
            backgroundColor: Samples.utils.transparentize(255, 242, 0, 0.3),
            fill: true
        }
    ]
    return datasets;
}

function initChartRTU(chart) {
    /* 
    init each chart with empty dataset, but proper labels from the home
    */
    if (chart.day !== null) { // first destroy previous charts if any
        chart.day.destroy();
    }

    // create empty charts with labels
    createChartDatasetpowersRTU();
  
    chart.day = new Chart(document.getElementById(`chartCanvas_RTU`).getContext('2d'), {
        type: 'line',
        data: {
            datasets: datasets
        },
        options: {
            responsive: true, 
            plugins: {
                legend: {
                    position: 'top'
                },
                tooltip: {
                    enabled: false
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'minute',
                        displayFormats: {
                            second: 'HH:mm:ss',
                            minute: 'HH:mm',
                            hour: 'HH',
                            day: 'DD',
                            month: 'MM yyyy',
                            year: 'yyyy'
                        }
                    },
                    title: {
                        color: 'red',
                        display: true,
                        text: 'Time'
                    }
                },
                y: {
                    title: {
                        color: 'red',
                        display: true,
                        text: 'Power (Watts) - W'
                    }
                }
            },
        }
    });
}

function createPageRTU() {
    /* 
    when arriving in the main page, create the chart canvas and init charts.
    */
    charts_rtu.day = null;
  
    // powers data
    createChartCanvasRTU("power_data_charts_rtu");
    initChartRTU(charts_rtu);

}

function main() {
    document.getElementById("profil_badge").innerText = sessionStorage.getItem("username");

    createPageRTU();

    initFirstQueryRTU();

}

main();