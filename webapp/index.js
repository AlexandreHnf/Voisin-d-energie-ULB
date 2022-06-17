/* 
index.js -> login page
author : Alexandre Heneffe. 
*/

var login_button = document.getElementById("submit");
var already_exist = false;
var login_result = [];


async function sendUsername(username) {
	/* 
	Send username to server
	*/
    const data = { username };
    const options = {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
    };
    const response = await fetch('/doesClientExist', options);
    const server_data = await response.json();
	already_exist = server_data.status;
    login_result = server_data.grp_ids;
}


function handlePreConnection(username) {
	/* 
	Setup the username for the next page
	+ get the group of installations ids associated to this user 
	*/
	sessionStorage.setItem("username", username);  // store username in session storage for next page
    // get group ids of this home = installations ids related to this home.
    var grp_ids = [];
    if (login_result.rows.length > 0) {
        for (let i = 0; i < login_result.rows[0].installations.length; i++) {
            install_id = login_result.rows[0].installations[i];
            if (install_id != username) {
                grp_ids.push(install_id);
            }
        }
    }
    // console.log(grp_ids);
    sessionStorage.setItem("grp_ids", JSON.stringify(grp_ids));

    // move to next page (main page)
	location.href = "client.html";
}


function validateLoginForm() {
	/* 
	check if the provided username is valid (is in the system or not)
	*/
    var username = document.getElementById("uname").value.toUpperCase();

    sendUsername(username);
    
    setTimeout(function() { // wait so that the receive has time to complete
        var text;
        if (username == "") {
            text = "Champ vide. Veuillez entrer un nom d'utilisateur valide.";
        } else if (already_exist == false) { // if does not have an account yet
            text = "Cet utilisateur ne figure pas dans le système."
        } else {
            text = "OK";
            handlePreConnection(username);
        }
        document.getElementById("login_err_msg").innerHTML = text;
    }, 400);

}