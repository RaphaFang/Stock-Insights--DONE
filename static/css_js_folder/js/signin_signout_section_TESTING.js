"use strict";
// ! submitSigninForm
async function submitSigninForm() {
  const userInfo = JSON.parse(localStorage.getItem("userInfo"));
  if (!userInfo) {
    const form = document.getElementById("signin-form");
    const signinFormData = new FormData(form);
    let ifSuccessMessage = "Sign in successfully.";
    const jsonData = convertToJson(signinFormData);
    await tokenPut(jsonData);
    await tokenGet(ifSuccessMessage);
  }
}

// ! submitSignUpForm
async function submitSignUpForm() {
  let userInfo = JSON.parse(localStorage.getItem("userInfo"));
  if (!userInfo) {
    const form = document.getElementById("signup-form");
    const signupFormData = new FormData(form);
    const jsonData = convertToJson(signupFormData);
    const response = await fetch("/tdt/v1/api/user", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(jsonData),
    });
    const result = await response.json();
    if (response.ok) {
      console.log("submitSignUpForm() -> user sign-up : ", response);
      let ifSuccessMessage = "Sign up successfully, automatically sign-in.";
      await tokenGet(ifSuccessMessage);
      displayLoginMessage(ifSuccessMessage);
    } else {
      console.error("Error:", result.message);
      displayLoginMessage(result.message);
    }
  }
}

// ! tokenPut
async function tokenPut(inputJsonHere) {
  const response = await fetch("/tdt/v1/api/user/auth", {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(inputJsonHere),
    credentials: "include",
  });
  const result = await response.json();
  if (response.ok) {
    console.log("tokenPut() -> user sign-in, return encode.token");
  } else {
    console.error("Error:", result.message);
    displayLoginMessage(result.message);
  }
}

// !  tokenGet
async function tokenGet(successMessage) {
  const response = await fetch("/tdt/v1/api/user/auth", {
    method: "GET",
    credentials: "include",
  });
  const result = await response.json();
  if (response.ok) {
    if (result["data"]) {
      const backHeight = document.getElementById("signin-form-div");
      if (backHeight) {
        backHeight.style.height = "350px";
      }
      const emailPart = document.getElementById("fieldin-email");
      if (emailPart) {
        emailPart.hidden = true;
      }
      console.log("tokenGet() -> user token checked, return user_info :", result);
      displayLoginMessage(successMessage);
      localStorage.setItem("userInfo", JSON.stringify(result.data));
    } else {
      console.log("tokenGet lunched, but there is no token from this user...");
      localStorage.removeItem("userInfo");
    }
  } else {
    console.error("Error:", result.message);
    displayLoginMessage(result.message);
    localStorage.removeItem("userInfo");
  }
  signinOutSwitch();
}

// convertToJson(), convert form data to json
function convertToJson(formDataInput) {
  const jsonData = {};
  formDataInput.forEach((value, key) => {
    jsonData[key] = value;
  });
  return jsonData;
}

//  displayLoginMessage(), display the correct or error below the form section
function displayLoginMessage(message) {
  let errorCatchers = document.querySelectorAll(".error-catcher");
  errorCatchers.forEach((element) => {
    element.hidden = false;
    element.textContent = message;
  });
}
// signinOutSwitch(), switch the btn at the up-right corner, and hide the signUpInSwitch()
document.addEventListener("DOMContentLoaded", async function () {
  await tokenGet("");
  signinOutSwitch();
});
function signinOutSwitch() {
  let userInfo = JSON.parse(localStorage.getItem("userInfo"));
  let switchFormDiv = document.querySelectorAll(".switch-form");
  const login = document.getElementById("login-btn");
  const logout = document.getElementById("logout-btn");
  if (userInfo) {
    login.hidden = true;
    logout.hidden = false;
    logout.textContent = "Sign-out";
    switchFormDiv.forEach((element) => {
      element.hidden = true;
    });
  } else {
    login.hidden = false;
    logout.hidden = true;
    login.textContent = "Sign-in";
  }
}
// if user state wasn't correct
async function deleteUserInfo() {
  const response = await fetch("/tdt/v1/api/user/logout", {
    method: "POST",
    credentials: "include",
  });
  if (response.ok) {
    localStorage.removeItem("userInfo");
    localStorage.removeItem("journeyRaw");
    localStorage.removeItem("journeyVerified");
    window.location.href = "/tdt/v1/";
  } else {
    console.error("Failed to log out:", await response.json());
  }
}

// userSignOut(), delete all the user info, refresh the page
function userSignOut() {
  if (confirm("You're about to sign out from this page, do you want to sign out?")) {
    deleteUserInfo();
  } else {
    console.log("userSignOut() denied -> token and user_info remained");
  }
}
// signUpInSwitch(), switch the form to display
function signUpInSwitch() {
  const signinForm = document.getElementById("signin-form-div");
  const signupForm = document.getElementById("signup-form-div");
  const emailBlock = document.getElementById("fieldin-email");
  const backHeight = document.getElementById("signin-form-div");
  const ec = document.getElementById("error-catcher");

  if (signupForm.hidden) {
    console.log("signinForm set hidden");
    signupForm.hidden = false;
    signinForm.hidden = true;
    emailBlock.hidden = true;

    emailBlock.hidden = true;
    ec.hidden = true;
    backHeight.style.height = "400px";

    hideErrorCatcher();
  } else {
    console.log("signupForm set hidden");
    signupForm.hidden = true;
    signinForm.hidden = false;
    hideErrorCatcher();
  }
}
// displaySignIn() login btn trigger, display sign in form
document.addEventListener("DOMContentLoaded", function () {
  displaySignIn();
});
function displaySignIn() {
  const loginBtn = document.getElementById("login-btn");
  const signinForm = document.getElementById("signin-form-div");
  const overlay = document.getElementById("overlay");
  if (loginBtn) {
    loginBtn.addEventListener("click", async function () {
      signinForm.hidden = false;
      overlay.hidden = false;
    });
  }
}

// cancelFormDisplay() close the form and reset the 'error-catcher' to empty
function cancelFormDisplay() {
  const signinForm = document.getElementById("signin-form-div");
  const signupForm = document.getElementById("signup-form-div");
  const overlay = document.getElementById("overlay");

  const emailBlock = document.getElementById("fieldin-email");
  const backHeight = document.getElementById("signin-form-div");
  const ec = document.getElementById("error-catcher");
  let infoDisplay = document.getElementById("fieldin-email-p");

  const passForm = document.getElementById("submit-password-div");
  const passFormErrorCatcher = document.getElementById("submit-password-error-catcher");

  signupForm.hidden = true;
  signinForm.hidden = true;
  overlay.hidden = true;

  emailBlock.hidden = true;
  ec.hidden = true;
  infoDisplay.innerText = "";
  backHeight.style.height = "400px";

  passForm.hidden = true;
  passFormErrorCatcher.innerText = "";

  hideErrorCatcher();
}
function hideErrorCatcher() {
  const errorCatchers = document.querySelectorAll(".error-catcher");
  errorCatchers.forEach((element) => {
    element.hidden = true;
  });
}

function forgetPassword() {
  const emailBlock = document.getElementById("fieldin-email");
  const backHeight = document.getElementById("signin-form-div");

  const ec = document.getElementById("error-catcher");
  let infoDisplay = document.getElementById("fieldin-email-p");

  if (emailBlock.hidden) {
    emailBlock.hidden = false;
    ec.hidden = true;
    infoDisplay.innerText = "";
    backHeight.style.height = "520px";
  } else {
    emailBlock.hidden = true;
    ec.hidden = true;
    infoDisplay.innerText = "";

    backHeight.style.height = "400px";
  }
}

async function sendVerifyToken() {
  let infoDisplay = document.getElementById("fieldin-email-p");
  const data = {
    email: document.getElementById("change_password").value,
  };
  const response = await fetch("/tdt/v1/api/user/reset_request", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  });

  const result = await response.json();
  console.log(result);
  if (response.ok) {
    infoDisplay.innerText = "Verify url had send to your address.";
  } else {
    infoDisplay.innerText = result.message;
    console.error("Error:", result.message);
  }
}

document.addEventListener("DOMContentLoaded", function () {
  const urlParams = new URLSearchParams(window.location.search);
  const status = urlParams.get("reset_password_token_status");
  const passForm = document.getElementById("submit-password-div");
  const overlay = document.getElementById("overlay");
  if (status) {
    if (status === "success") {
      overlay.hidden = false;
      passForm.hidden = false;
    }
    if (status === "error") {
      overlay.hidden = true;
      passForm.hidden = true;
      alert("The token can't be verified, please re-access the url from your email.");
    }
  }
});

async function resetPassword() {
  let firstPassword = document.getElementById("submit-password-first").value;
  let secondPassword = document.getElementById("submit-password-second").value;
  if (firstPassword === secondPassword) {
    const form = document.getElementById("submit-password-form");
    const changePassFormData = new FormData(form);
    const jsonData = convertToJson(changePassFormData);
    const errorCatcher = document.getElementById("submit-password-error-catcher");
    const response = await fetch("/tdt/v1/api/user/reset_password", {
      method: "PUT",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(jsonData),
    });
    const result = await response.json();
    console.log(result);
    if (response.ok) {
      errorCatcher.hidden = false;
      errorCatcher.innerText = result.message;
      alert(result.message);
      window.location.href = "/tdt/v1/";
    } else {
      errorCatcher.hidden = false;
      errorCatcher.innerText = result.message;
      console.error("Error:", result.message);
    }
  } else {
    alert("Please make sure the two passwords are the same.");
  }
}
