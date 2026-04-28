(function () {
  var script = document.currentScript || {};
  var apiBase = script.getAttribute && script.getAttribute("data-api-base");
  var organisationId = script.getAttribute && script.getAttribute("data-organisation-id");
  var title = (script.getAttribute && script.getAttribute("data-title")) || "Property assistant";
  var api = apiBase || "https://booking-system-b13f.onrender.com";
  var sessionToken = "";
  var isOpen = false;

  function el(tag, attrs, text) {
    var node = document.createElement(tag);
    var key;
    for (key in attrs || {}) {
      if (key === "style") node.setAttribute("style", attrs[key]);
      else if (key === "className") node.className = attrs[key];
      else node.setAttribute(key, attrs[key]);
    }
    if (text) node.textContent = text;
    return node;
  }

  function post(path, body) {
    return fetch(api + path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {})
    }).then(function (res) {
      return res.json().then(function (data) {
        if (!res.ok) throw new Error(data.error || "Request failed");
        return data;
      });
    });
  }

  function addMessage(list, role, message) {
    var bubble = el("div", {
      style:
        "max-width:86%;padding:10px 12px;border-radius:12px;margin:8px 0;line-height:1.4;font-size:14px;" +
        (role === "user"
          ? "margin-left:auto;background:#12344d;color:#fff;"
          : "margin-right:auto;background:#f4f7f8;color:#243342;border:1px solid #d7dee7;")
    }, message);
    list.appendChild(bubble);
    list.scrollTop = list.scrollHeight;
  }

  function setBusy(input, button, busy) {
    input.disabled = busy;
    button.disabled = busy;
    button.style.opacity = busy ? "0.65" : "1";
  }

  function openChat(panel, launcher, list, input) {
    if (isOpen) return;
    isOpen = true;
    panel.style.display = "flex";
    launcher.style.display = "none";
    input.focus();

    if (sessionToken) return;

    post("/api/chatbot/start", {
      organisation_id: organisationId || null,
      source_page: window.location.href
    }).then(function (data) {
      sessionToken = data.session_token;
      addMessage(list, "assistant", data.message);
    }).catch(function () {
      addMessage(list, "assistant", "I can’t start the chat right now. Please try again shortly.");
    });
  }

  function init() {
    var root = el("div", {
      style: "position:fixed;right:18px;bottom:18px;z-index:99999;font-family:Arial,sans-serif;"
    });

    var launcher = el("button", {
      type: "button",
      style: "border:none;background:#12344d;color:#fff;border-radius:999px;padding:13px 16px;box-shadow:0 10px 30px rgba(18,52,77,.22);cursor:pointer;font-weight:bold;"
    }, "Chat property plans");

    var panel = el("div", {
      style: "display:none;flex-direction:column;width:min(380px,calc(100vw - 28px));height:min(620px,calc(100vh - 28px));background:#fff;border:1px solid #d7dee7;border-radius:10px;box-shadow:0 18px 50px rgba(18,52,77,.22);overflow:hidden;"
    });

    var header = el("div", {
      style: "display:flex;align-items:center;justify-content:space-between;gap:12px;padding:14px 16px;background:#12344d;color:#fff;"
    });
    header.appendChild(el("div", { style: "font-weight:bold;" }, title));
    var close = el("button", {
      type: "button",
      style: "border:1px solid rgba(255,255,255,.35);background:transparent;color:#fff;border-radius:8px;padding:6px 8px;cursor:pointer;"
    }, "Close");
    header.appendChild(close);

    var list = el("div", {
      style: "flex:1;padding:14px;overflow:auto;background:#fff;"
    });

    var form = el("form", {
      style: "display:flex;gap:8px;padding:12px;border-top:1px solid #d7dee7;background:#f9fbfc;"
    });
    var input = el("input", {
      type: "text",
      placeholder: "Type your reply...",
      style: "flex:1;min-width:0;border:1px solid #d7dee7;border-radius:8px;padding:11px;font-size:14px;"
    });
    var send = el("button", {
      type: "submit",
      style: "border:none;background:#2a7f9e;color:#fff;border-radius:8px;padding:0 14px;cursor:pointer;font-weight:bold;"
    }, "Send");
    form.appendChild(input);
    form.appendChild(send);

    form.onsubmit = function (event) {
      var message = input.value.trim();
      event.preventDefault();
      if (!message || !sessionToken) return;
      input.value = "";
      addMessage(list, "user", message);
      setBusy(input, send, true);
      post("/api/chatbot/message", {
        session_token: sessionToken,
        message: message
      }).then(function (data) {
        addMessage(list, "assistant", data.message || "Thanks.");
        setBusy(input, send, false);
      }).catch(function (err) {
        addMessage(list, "assistant", err.message || "Something went wrong. Please try again.");
        setBusy(input, send, false);
      });
    };

    close.onclick = function () {
      panel.style.display = "none";
      launcher.style.display = "block";
      isOpen = false;
      if (sessionToken) {
        post("/api/chatbot/end", { session_token: sessionToken }).catch(function () {});
      }
    };

    launcher.onclick = function () {
      openChat(panel, launcher, list, input);
    };

    panel.appendChild(header);
    panel.appendChild(list);
    panel.appendChild(form);
    root.appendChild(launcher);
    root.appendChild(panel);
    document.body.appendChild(root);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
