var login_button = document.getElementById("submit");

function validateLoginForm() {
	var username = document.getElementById("uname").value;
	var password = document.getElementById("pwd").value;
	var ok = true;
	if (username == "flukso_admin" && password == "789") {
		text = "OK admin";
	} else if (username != "flukso_admin" && password == "123") {
		text = "OK user";
	} else {
		text = "Invalid inputs";
		ok = false;
	}
	if (ok) {
		console.log("ON SWITCH VERS CLIENT");
		sessionStorage.setItem("username", username);  // store username in session storage for next page
		location.href = "client.html";
	}
	document.getElementById("login_err_msg").innerHTML = text;
}

function validateRegisterForm() {
	var username = document.getElementById("register_uname").value;
	var password = document.getElementById("register_pwd").value;
	if (username == "flukso_admin" && password != "") {
		text = "OK admin";
	} else if (username != "flukso_admin" && password != "") {
		text = "OK user";
	} else {
		text = "Invalid inputs";
	}
	document.getElementById("register_err_msg").innerHTML = text;
}