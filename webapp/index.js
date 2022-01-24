var login_button = document.getElementById("submit");
var user_radio = document.getElementByName("user_radio");
var admin_radio = document.getElementByName("admin_radio");

function validateLoginForm() {
	var input_value = document.getElementById("userinput")  
	if (input_value == "") {
		text = "Veuillez fournir une entrée."
	} else if (admin_radio.checked && input_value == "789"){
		text = "OK admin";
	} else if (user_radio.checked) {
		text = "OK user";
	}
	document.getElementById("login_err_msg").innerHTML = text;
}