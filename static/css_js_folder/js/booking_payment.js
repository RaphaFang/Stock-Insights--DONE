"use strict";
document.addEventListener("DOMContentLoaded", function () {
  let cardFields = {
    number: {
      element: "#card-number",
      placeholder: "**** **** **** ****",
    },
    expirationDate: {
      element: "#card-expiration-date",
      placeholder: "MM / YY",
    },
    ccv: {
      element: "#card-ccv",
      placeholder: "CCV",
    },
  };

  TPDirect.card.setup({
    fields: cardFields,
    styles: {
      "input.card-number": {},
      "input.expiration-date": {},
      ":focus": {},
      ".valid": {
        color: "green",
      },
      ".invalid": {
        color: "red",
      },
      "@media screen and (max-width: 400px)": {
        input: {
          color: "orange",
        },
      },
    },
    isMaskCreditCardNumber: true,
    maskCreditCardNumberRange: {
      beginIndex: 6,
      endIndex: 11,
    },
  });

  const cardTypes = ["mastercard", "visa", "jcb", "amex", "unionpay", "unknown"];
  TPDirect.card.onUpdate(function (update) {
    const submitButton = document.getElementById("submit-button");
    if (update && update.canGetPrime) {
      submitButton.removeAttribute("disabled");
    } else {
      submitButton.setAttribute("disabled", true);
    }
    if (update && cardTypes.includes(update.cardType)) {
      switch (update.cardType) {
        case "visa":
          TPDirect.ccv.setupCardType(TPDirect.CardType.VISA);
          break;
        case "mastercard":
          TPDirect.ccv.setupCardType(TPDirect.CardType.MASTERCARD);
          break;
        case "jcb":
          TPDirect.ccv.setupCardType(TPDirect.CardType.JCB);
          break;
        case "amex":
          TPDirect.ccv.setupCardType(TPDirect.CardType.AMEX);
          break;
        case "unionpay":
          TPDirect.ccv.setupCardType(TPDirect.CardType.UNIONPAY);
          break;
        case "unknown":
          TPDirect.ccv.setupCardType(TPDirect.CardType.UNKNOWN);
          break;
      }
    }
  });

  document.getElementById("submit-button").addEventListener("click", onSubmit);
});

function onSubmit(event) {
  event.preventDefault();
  const tappayStatus = TPDirect.card.getTappayFieldsStatus();
  if (tappayStatus.canGetPrime === false) {
    alert("can not get prime");
    return;
  }
  TPDirect.card.getPrime(function (cardResult) {
    if (cardResult.status !== 0) {
      console.log("get card prime error, " + cardResult.msg);
      return;
    }
    console.log("get card prime successfully, prime: " + cardResult.card.prime);
    orderPost(cardResult.card.prime);
  });
}
// 放棄使用CCV prime，始終無法有效建立
async function orderPost(prime) {
  const contactName = document.getElementById("book-personal-info-place-name").value;
  const contactEmail = document.getElementById("book-personal-info-place-email").value;
  const contactTel = document.getElementById("book-personal-info-place-tel").value;
  const verData = JSON.parse(localStorage.getItem("journeyVerified"));

  let contactAndPrimeForm = {
    prime: prime,
    name: contactName,
    email: contactEmail,
    phone: contactTel,
    price: verData.price,
  };
  console.log(contactAndPrimeForm);

  const response = await fetch("/tdt/v1/api/orders", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    credentials: "include",
    body: JSON.stringify(contactAndPrimeForm),
  });

  const result = await response.json();
  if (response.ok) {
    console.log("bookingPost() -> success:", result.data);
    if (result.data.number) {
      window.location.href = `/tdt/v1/thankyou?number=${result.data.number}`;
    }
  } else {
    if (response.status === 403) {
      bookingDisplaySignIn();
    } else if (response.status === 400) {
      alert("Facing error booking this trip, " + result.message + ".");
    } else if (response.status === 422) {
      alert("Invalid user-info detected, please make sure the correct formate.");
      console.log(result.message);
    }
    console.log(result.message);
  }
}
