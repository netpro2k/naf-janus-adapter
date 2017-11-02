/******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, {
/******/ 				configurable: false,
/******/ 				enumerable: true,
/******/ 				get: getter
/******/ 			});
/******/ 		}
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = 0);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/***/ (function(module, exports, __webpack_require__) {

let getMicrophone = (() => {
  var _ref = _asyncToGenerator(function* () {
    try {
      return yield navigator.mediaDevices.getUserMedia({
        audio: true
      });
    } catch (e) {
      if (e.name === "NotAllowedError") {
        console.warn("Microphone access not allowed.");
      } else {
        console.error(e);
      }
    }
  });

  return function getMicrophone() {
    return _ref.apply(this, arguments);
  };
})();

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

var mj = __webpack_require__(1);

const ContentKind = {
  Audio: 1,
  Video: 2,
  Data: 4
};

function randomUint() {
  return Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);
}

function waitForEvent(target, event) {
  return new Promise((resolve, reject) => {
    target.addEventListener(event, e => resolve(e), { once: true });
  });
}

const PEER_CONNECTION_CONFIG = {
  iceServers: [{ urls: "stun:stun1.l.google.com:19302" }, { urls: "stun:stun2.l.google.com:19302" }]
};

class JanusAdapter {
  constructor() {
    this.room = null;
    this.userId = randomUint();

    this.serverUrl = null;
    this.webRtcOptions = {};
    this.ws = null;
    this.session = null;

    this.publisher = null;
    this.occupants = {};
    this.occupantPromises = {};

    this.onWebsocketMessage = this.onWebsocketMessage.bind(this);
    this.onDataChannelMessage = this.onDataChannelMessage.bind(this);
  }

  setServerUrl(url) {
    this.serverUrl = url;
  }

  setApp(app) {}

  setRoom(roomName) {
    try {
      this.room = parseInt(roomName);
    } catch (e) {
      throw new Error("Room must be a positive integer.");
    }
  }

  setWebRtcOptions(options) {
    this.webRtcOptions = options;
  }

  setServerConnectListeners(successListener, failureListener) {
    this.connectSuccess = successListener;
    this.connectFailure = failureListener;
  }

  setRoomOccupantListener(occupantListener) {
    this.onOccupantsChanged = occupantListener;
  }

  setDataChannelListeners(openListener, closedListener, messageListener) {
    this.onOccupantConnected = openListener;
    this.onOccupantDisconnected = closedListener;
    this.onOccupantMessage = messageListener;
  }

  connect() {
    this.ws = new WebSocket(this.serverUrl, "janus-protocol");
    this.session = new mj.JanusSession(this.ws.send.bind(this.ws));
    this.ws.addEventListener("open", _ => this.onWebsocketOpen());
    this.ws.addEventListener("message", this.onWebsocketMessage);
  }

  onWebsocketOpen() {
    var _this = this;

    return _asyncToGenerator(function* () {
      // Create the Janus Session
      yield _this.session.create();

      // Attach the SFU Plugin and create a RTCPeerConnection for the publisher.
      // The publisher sends audio and opens two bidirectional data channels.
      // One reliable datachannel and one unreliable.
      var publisherPromise = _this.createPublisher();
      _this.occupantPromises[_this.userId] = publisherPromise;
      _this.publisher = yield publisherPromise;

      _this.connectSuccess(_this.userId);

      // Add all of the initial occupants.
      for (let occupantId of _this.publisher.initialOccupants) {
        if (occupantId !== _this.userId) {
          _this.occupantPromises[occupantId] = _this.addOccupant(occupantId);
        }
      }
    })();
  }

  onWebsocketMessage(event) {
    var message = JSON.parse(event.data);
    this.session.receive(message);

    // Handle all of the join and leave events from the publisher.
    if (message.plugindata && message.plugindata.data) {
      var data = message.plugindata.data;

      if (data.event === "join") {
        this.occupantPromises[data.user_id] = this.addOccupant(data.user_id);
      } else if (data.event && data.event === "leave") {
        this.removeOccupant(data.user_id);
      }
    }
  }

  addOccupant(occupantId) {
    var _this2 = this;

    return _asyncToGenerator(function* () {
      var subscriber = yield _this2.createSubscriber(occupantId);
      // Call the Networked AFrame callbacks for the new occupant.
      _this2.onOccupantConnected(occupantId);
      _this2.occupants[occupantId] = true;
      _this2.onOccupantsChanged(_this2.occupants);
      return subscriber;
    })();
  }

  removeOccupant(occupantId) {
    if (this.occupants[occupantId]) {
      delete this.occupants[occupantId];
      // Call the Networked AFrame callbacks for the removed occupant.
      this.onOccupantDisconnected(occupantId);
      this.onOccupantsChanged(this.occupants);
    }
  }

  createPublisher() {
    var _this3 = this;

    return _asyncToGenerator(function* () {
      var handle = new mj.JanusPluginHandle(_this3.session);
      yield handle.attach("janus.plugin.sfu");

      var peerConnection = new RTCPeerConnection(PEER_CONNECTION_CONFIG);

      peerConnection.addEventListener("icecandidate", function (event) {
        handle.sendTrickle(event.candidate);
      });

      // Create an unreliable datachannel for sending and receiving component updates, etc.
      var unreliableChannel = peerConnection.createDataChannel("unreliable", {
        ordered: false,
        maxRetransmits: 0
      });
      unreliableChannel.addEventListener("message", _this3.onDataChannelMessage);

      // Create a reliable datachannel for sending and recieving entity instantiations, etc.
      var reliableChannel = peerConnection.createDataChannel("reliable", {
        ordered: true
      });
      reliableChannel.addEventListener("message", _this3.onDataChannelMessage);

      var mediaStream;
      if (_this3.webRtcOptions.audio) {
        mediaStream = yield getMicrophone();

        if (mediaStream) {
          peerConnection.addStream(mediaStream);
        }
      }

      var offer = yield peerConnection.createOffer();
      yield peerConnection.setLocalDescription(offer);

      var answer = yield handle.sendJsep(offer);
      yield peerConnection.setRemoteDescription(answer.jsep);

      // Wait for the reliable datachannel to be open before we start sending messages on it.
      yield waitForEvent(reliableChannel, "open");

      // Send join message to janus. Listen for join/leave messages. Automatically subscribe to all users' WebRTC data.
      var message = yield _this3.sendJoin(handle, _this3.room, _this3.userId, true);

      var initialOccupants = message.plugindata.data.response.user_ids;

      return {
        handle,
        initialOccupants,
        reliableChannel,
        unreliableChannel,
        mediaStream,
        peerConnection
      };
    })();
  }

  createSubscriber(occupantId) {
    var _this4 = this;

    return _asyncToGenerator(function* () {
      var handle = new mj.JanusPluginHandle(_this4.session);
      yield handle.attach("janus.plugin.sfu");

      var peerConnection = new RTCPeerConnection(PEER_CONNECTION_CONFIG);

      peerConnection.addEventListener("icecandidate", function (event) {
        handle.sendTrickle(event.candidate);
      });

      var offer = yield peerConnection.createOffer({
        offerToReceiveAudio: true
      });

      yield peerConnection.setLocalDescription(offer);
      var answer = yield handle.sendJsep(offer);
      yield peerConnection.setRemoteDescription(answer.jsep);

      // Send join message to janus. Don't listen for join/leave messages. Subscribe to the occupant's audio stream.
      yield _this4.sendJoin(handle, _this4.room, _this4.userId, false, [{
        publisher_id: occupantId,
        content_kind: ContentKind.Audio
      }]);

      // Get the occupant's audio stream.
      var streams = peerConnection.getRemoteStreams();
      var mediaStream = streams.length > 0 ? streams[0] : null;

      return {
        handle,
        mediaStream,
        peerConnection
      };
    })();
  }

  sendJoin(handle, roomId, userId, notify, specs) {
    return handle.sendMessage({
      kind: "join",
      room_id: roomId,
      user_id: userId,
      notify,
      subscription_specs: specs
    });
  }

  onDataChannelMessage(event) {
    var message = JSON.parse(event.data);

    if (message.dataType) {
      this.onOccupantMessage(null, message.dataType, message.data);
    }
  }

  shouldStartConnectionTo(clientId) {
    return true;
  }

  startStreamConnection(clientId) {}

  closeStreamConnection(clientId) {}

  getConnectStatus(clientId) {
    if (this.occupants[clientId]) {
      return NAF.adapters.IS_CONNECTED;
    } else {
      return NAF.adapters.NOT_CONNECTED;
    }
  }

  getMediaStream(clientId) {
    var occupantPromise = this.occupantPromises[clientId];

    if (!occupantPromise) {
      return Promise.reject(new Error(`Subscriber for client: ${clientId} does not exist.`));
    }

    return occupantPromise.then(s => s.mediaStream);
  }

  enableMicrophone(enabled) {
    if (this.publisher && this.publisher.mediaStream) {
      var audioTracks = this.publisher.mediaStream.getAudioTracks();

      if (audioTracks.length > 0) {
        audioTracks[0].enabled = enabled;
      }
    }
  }

  sendData(clientId, dataType, data) {
    this.publisher.unreliableChannel.send(JSON.stringify({ clientId, dataType, data }));
  }

  sendDataGuaranteed(clientId, dataType, data) {
    this.publisher.reliableChannel.send(JSON.stringify({ clientId, dataType, data }));
  }

  broadcastData(dataType, data) {
    this.publisher.unreliableChannel.send(JSON.stringify({ dataType, data }));
  }

  broadcastDataGuaranteed(dataType, data) {
    this.publisher.reliableChannel.send(JSON.stringify({ dataType, data }));
  }
}

NAF.adapters.register("janus", JanusAdapter);

module.exports = JanusAdapter;

/***/ }),
/* 1 */
/***/ (function(module, exports) {

/** Whether to log information about incoming and outgoing Janus signals. **/
var verbose = false;

/**
 * Represents a handle to a single Janus plugin on a Janus session. Each WebRTC connection to the Janus server will be
 * associated with a single handle. Once attached to the server, this handle will be given a unique ID which should be
 * used to associate it with future signalling messages.
 *
 * See https://janus.conf.meetecho.com/docs/rest.html#handles.
 **/
function JanusPluginHandle(session) {
  this.session = session;
  this.id = undefined;
}

/** Attaches this handle to the Janus server and sets its ID. **/
JanusPluginHandle.prototype.attach = function(plugin) {
  var payload = { janus: "attach", plugin: plugin, "force-bundle": true, "force-rtcp-mux": true };
  return this.session.send(payload).then(resp => {
    this.id = resp.data.id;
    return resp;
  });
};

/** Detaches this handle. **/
JanusPluginHandle.prototype.detach = function() {
  return this.send({ janus: "detach" });
};

/**
 * Sends a signal associated with this handle. Signals should be JSON-serializable objects. Returns a promise that will
 * be resolved or rejected when a response to this signal is received, or when no response is received within the
 * session timeout.
 **/
JanusPluginHandle.prototype.send = function(signal) {
  return this.session.send(Object.assign({ handle_id: this.id }, signal));
};

/** Sends a plugin-specific message associated with this handle. **/
JanusPluginHandle.prototype.sendMessage = function(body) {
  return this.send({ janus: "message", body: body });
};

/** Sends a JSEP offer or answer associated with this handle. **/
JanusPluginHandle.prototype.sendJsep = function(jsep) {
  return this.send({ janus: "message", body: {}, jsep: jsep });
};

/** Sends an ICE trickle candidate associated with this handle. **/
JanusPluginHandle.prototype.sendTrickle = function(candidate) {
  return this.send({ janus: "trickle",  candidate: candidate });
};

/**
 * Represents a Janus session -- a Janus context from within which you can open multiple handles and connections. Once
 * created, this session will be given a unique ID which should be used to associate it with future signalling messages.
 *
 * See https://janus.conf.meetecho.com/docs/rest.html#sessions.
 **/
function JanusSession(output, options) {
  this.output = output;
  this.id = undefined;
  this.nextTxId = 0;
  this.txns = {};
  this.options = options || {
    timeoutMs: 10000,
    keepaliveMs: 30000
  };
}

/** Creates this session on the Janus server and sets its ID. **/
JanusSession.prototype.create = function() {
  return this.send({ janus: "create" }).then(resp => {
    this.id = resp.data.id;
    return resp;
  });
};

/** Destroys this session. **/
JanusSession.prototype.destroy = function() {
  return this.send({ janus: "destroy" });
};

/**
 * Callback for receiving JSON signalling messages pertinent to this session. If the signals are responses to previously
 * sent signals, the promises for the outgoing signals will be resolved or rejected appropriately with this signal as an
 * argument.
 *
 * External callers should call this function every time a new signal arrives on the transport; for example, in a
 * WebSocket's `message` event, or when a new datum shows up in an HTTP long-polling response.
 **/
JanusSession.prototype.receive = function(signal) {
  if (module.exports.verbose) {
    console.debug("Incoming Janus signal: ", signal);
  }
  if (signal.transaction != null) {
    var handlers = this.txns[signal.transaction];
    if (signal.janus === "ack" && signal.hint) {
      // this is an ack of an asynchronously-processed request, we should wait
      // to resolve the promise until the actual response comes in
    } else if (handlers != null) {
      if (handlers.timeout != null) {
        clearTimeout(handlers.timeout);
      }
      delete this.txns[signal.transaction];
      (signal.janus === "error" ? handlers.reject : handlers.resolve)(signal);
    }
  }
};

/**
 * Sends a signal associated with this session. Signals should be JSON-serializable objects. Returns a promise that will
 * be resolved or rejected when a response to this signal is received, or when no response is received within the
 * session timeout.
 **/
JanusSession.prototype.send = function(signal) {
  if (module.exports.verbose) {
    console.debug("Outgoing Janus signal: ", signal);
  }
  signal = Object.assign({
    session_id: this.id,
    transaction: (this.nextTxId++).toString()
  }, signal);
  return new Promise((resolve, reject) => {
    var timeout = null;
    if (this.options.timeoutMs) {
      timeout = setTimeout(() => {
        delete this.txns[signal.transaction];
        reject(new Error("Signalling message timed out."));
      }, this.options.timeoutMs);
    }
    this.txns[signal.transaction] = { resolve: resolve, reject: reject, timeout: timeout };
    this.output(JSON.stringify(signal));
    this._resetKeepalive();
  });
};

JanusSession.prototype._resetKeepalive = function() {
  if (this.keepaliveTimeout) {
    clearTimeout(this.keepaliveTimeout);
  }
  if (this.options.keepaliveMs) {
    this.keepaliveTimeout = setTimeout(() => this._keepalive(), this.options.keepaliveMs);
  }
};

JanusSession.prototype._keepalive = function() {
  return this.send({ janus: "keepalive" });
};

module.exports = {
  JanusPluginHandle,
  JanusSession,
  verbose
};


/***/ })
/******/ ]);
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly8vd2VicGFjay9ib290c3RyYXAgMzIxMmI5MzEyOTI0ZjhmOTY2MDciLCJ3ZWJwYWNrOi8vLy4vc3JjL2luZGV4LmpzIiwid2VicGFjazovLy8uL25vZGVfbW9kdWxlcy9taW5pamFudXMvbWluaWphbnVzLmpzIl0sIm5hbWVzIjpbIm5hdmlnYXRvciIsIm1lZGlhRGV2aWNlcyIsImdldFVzZXJNZWRpYSIsImF1ZGlvIiwiZSIsIm5hbWUiLCJjb25zb2xlIiwid2FybiIsImVycm9yIiwiZ2V0TWljcm9waG9uZSIsIm1qIiwicmVxdWlyZSIsIkNvbnRlbnRLaW5kIiwiQXVkaW8iLCJWaWRlbyIsIkRhdGEiLCJyYW5kb21VaW50IiwiTWF0aCIsImZsb29yIiwicmFuZG9tIiwiTnVtYmVyIiwiTUFYX1NBRkVfSU5URUdFUiIsIndhaXRGb3JFdmVudCIsInRhcmdldCIsImV2ZW50IiwiUHJvbWlzZSIsInJlc29sdmUiLCJyZWplY3QiLCJhZGRFdmVudExpc3RlbmVyIiwib25jZSIsIlBFRVJfQ09OTkVDVElPTl9DT05GSUciLCJpY2VTZXJ2ZXJzIiwidXJscyIsIkphbnVzQWRhcHRlciIsImNvbnN0cnVjdG9yIiwicm9vbSIsInVzZXJJZCIsInNlcnZlclVybCIsIndlYlJ0Y09wdGlvbnMiLCJ3cyIsInNlc3Npb24iLCJwdWJsaXNoZXIiLCJvY2N1cGFudHMiLCJvY2N1cGFudFByb21pc2VzIiwib25XZWJzb2NrZXRNZXNzYWdlIiwiYmluZCIsIm9uRGF0YUNoYW5uZWxNZXNzYWdlIiwic2V0U2VydmVyVXJsIiwidXJsIiwic2V0QXBwIiwiYXBwIiwic2V0Um9vbSIsInJvb21OYW1lIiwicGFyc2VJbnQiLCJFcnJvciIsInNldFdlYlJ0Y09wdGlvbnMiLCJvcHRpb25zIiwic2V0U2VydmVyQ29ubmVjdExpc3RlbmVycyIsInN1Y2Nlc3NMaXN0ZW5lciIsImZhaWx1cmVMaXN0ZW5lciIsImNvbm5lY3RTdWNjZXNzIiwiY29ubmVjdEZhaWx1cmUiLCJzZXRSb29tT2NjdXBhbnRMaXN0ZW5lciIsIm9jY3VwYW50TGlzdGVuZXIiLCJvbk9jY3VwYW50c0NoYW5nZWQiLCJzZXREYXRhQ2hhbm5lbExpc3RlbmVycyIsIm9wZW5MaXN0ZW5lciIsImNsb3NlZExpc3RlbmVyIiwibWVzc2FnZUxpc3RlbmVyIiwib25PY2N1cGFudENvbm5lY3RlZCIsIm9uT2NjdXBhbnREaXNjb25uZWN0ZWQiLCJvbk9jY3VwYW50TWVzc2FnZSIsImNvbm5lY3QiLCJXZWJTb2NrZXQiLCJKYW51c1Nlc3Npb24iLCJzZW5kIiwiXyIsIm9uV2Vic29ja2V0T3BlbiIsImNyZWF0ZSIsInB1Ymxpc2hlclByb21pc2UiLCJjcmVhdGVQdWJsaXNoZXIiLCJvY2N1cGFudElkIiwiaW5pdGlhbE9jY3VwYW50cyIsImFkZE9jY3VwYW50IiwibWVzc2FnZSIsIkpTT04iLCJwYXJzZSIsImRhdGEiLCJyZWNlaXZlIiwicGx1Z2luZGF0YSIsInVzZXJfaWQiLCJyZW1vdmVPY2N1cGFudCIsInN1YnNjcmliZXIiLCJjcmVhdGVTdWJzY3JpYmVyIiwiaGFuZGxlIiwiSmFudXNQbHVnaW5IYW5kbGUiLCJhdHRhY2giLCJwZWVyQ29ubmVjdGlvbiIsIlJUQ1BlZXJDb25uZWN0aW9uIiwic2VuZFRyaWNrbGUiLCJjYW5kaWRhdGUiLCJ1bnJlbGlhYmxlQ2hhbm5lbCIsImNyZWF0ZURhdGFDaGFubmVsIiwib3JkZXJlZCIsIm1heFJldHJhbnNtaXRzIiwicmVsaWFibGVDaGFubmVsIiwibWVkaWFTdHJlYW0iLCJhZGRTdHJlYW0iLCJvZmZlciIsImNyZWF0ZU9mZmVyIiwic2V0TG9jYWxEZXNjcmlwdGlvbiIsImFuc3dlciIsInNlbmRKc2VwIiwic2V0UmVtb3RlRGVzY3JpcHRpb24iLCJqc2VwIiwic2VuZEpvaW4iLCJyZXNwb25zZSIsInVzZXJfaWRzIiwib2ZmZXJUb1JlY2VpdmVBdWRpbyIsInB1Ymxpc2hlcl9pZCIsImNvbnRlbnRfa2luZCIsInN0cmVhbXMiLCJnZXRSZW1vdGVTdHJlYW1zIiwibGVuZ3RoIiwicm9vbUlkIiwibm90aWZ5Iiwic3BlY3MiLCJzZW5kTWVzc2FnZSIsImtpbmQiLCJyb29tX2lkIiwic3Vic2NyaXB0aW9uX3NwZWNzIiwiZGF0YVR5cGUiLCJzaG91bGRTdGFydENvbm5lY3Rpb25UbyIsImNsaWVudElkIiwic3RhcnRTdHJlYW1Db25uZWN0aW9uIiwiY2xvc2VTdHJlYW1Db25uZWN0aW9uIiwiZ2V0Q29ubmVjdFN0YXR1cyIsIk5BRiIsImFkYXB0ZXJzIiwiSVNfQ09OTkVDVEVEIiwiTk9UX0NPTk5FQ1RFRCIsImdldE1lZGlhU3RyZWFtIiwib2NjdXBhbnRQcm9taXNlIiwidGhlbiIsInMiLCJlbmFibGVNaWNyb3Bob25lIiwiZW5hYmxlZCIsImF1ZGlvVHJhY2tzIiwiZ2V0QXVkaW9UcmFja3MiLCJzZW5kRGF0YSIsInN0cmluZ2lmeSIsInNlbmREYXRhR3VhcmFudGVlZCIsImJyb2FkY2FzdERhdGEiLCJicm9hZGNhc3REYXRhR3VhcmFudGVlZCIsInJlZ2lzdGVyIiwibW9kdWxlIiwiZXhwb3J0cyJdLCJtYXBwaW5ncyI6IjtBQUFBO0FBQ0E7O0FBRUE7QUFDQTs7QUFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUFFQTtBQUNBOztBQUVBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBOzs7QUFHQTtBQUNBOztBQUVBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxhQUFLO0FBQ0w7QUFDQTs7QUFFQTtBQUNBO0FBQ0E7QUFDQSxtQ0FBMkIsMEJBQTBCLEVBQUU7QUFDdkQseUNBQWlDLGVBQWU7QUFDaEQ7QUFDQTtBQUNBOztBQUVBO0FBQ0EsOERBQXNELCtEQUErRDs7QUFFckg7QUFDQTs7QUFFQTtBQUNBOzs7Ozs7OzsrQkMzQ0EsYUFBK0I7QUFDN0IsUUFBSTtBQUNGLGFBQU8sTUFBTUEsVUFBVUMsWUFBVixDQUF1QkMsWUFBdkIsQ0FBb0M7QUFDL0NDLGVBQU87QUFEd0MsT0FBcEMs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QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQUVBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBLEdBQUc7QUFDSDtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxPQUFPO0FBQ1A7QUFDQSxxQ0FBcUM7QUFDckM7QUFDQTtBQUNBLEdBQUc7QUFDSDs7QUFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQUVBO0FBQ0Esb0JBQW9CLHFCQUFxQjtBQUN6Qzs7QUFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBIiwiZmlsZSI6Im5hZi1qYW51cy1hZGFwdGVyLmpzIiwic291cmNlc0NvbnRlbnQiOlsiIFx0Ly8gVGhlIG1vZHVsZSBjYWNoZVxuIFx0dmFyIGluc3RhbGxlZE1vZHVsZXMgPSB7fTtcblxuIFx0Ly8gVGhlIHJlcXVpcmUgZnVuY3Rpb25cbiBcdGZ1bmN0aW9uIF9fd2VicGFja19yZXF1aXJlX18obW9kdWxlSWQpIHtcblxuIFx0XHQvLyBDaGVjayBpZiBtb2R1bGUgaXMgaW4gY2FjaGVcbiBcdFx0aWYoaW5zdGFsbGVkTW9kdWxlc1ttb2R1bGVJZF0pIHtcbiBcdFx0XHRyZXR1cm4gaW5zdGFsbGVkTW9kdWxlc1ttb2R1bGVJZF0uZXhwb3J0cztcbiBcdFx0fVxuIFx0XHQvLyBDcmVhdGUgYSBuZXcgbW9kdWxlIChhbmQgcHV0IGl0IGludG8gdGhlIGNhY2hlKVxuIFx0XHR2YXIgbW9kdWxlID0gaW5zdGFsbGVkTW9kdWxlc1ttb2R1bGVJZF0gPSB7XG4gXHRcdFx0aTogbW9kdWxlSWQsXG4gXHRcdFx0bDogZmFsc2UsXG4gXHRcdFx0ZXhwb3J0czoge31cbiBcdFx0fTtcblxuIFx0XHQvLyBFeGVjdXRlIHRoZSBtb2R1bGUgZnVuY3Rpb25cbiBcdFx0bW9kdWxlc1ttb2R1bGVJZF0uY2FsbChtb2R1bGUuZXhwb3J0cywgbW9kdWxlLCBtb2R1bGUuZXhwb3J0cywgX193ZWJwYWNrX3JlcXVpcmVfXyk7XG5cbiBcdFx0Ly8gRmxhZyB0aGUgbW9kdWxlIGFzIGxvYWRlZFxuIFx0XHRtb2R1bGUubCA9IHRydWU7XG5cbiBcdFx0Ly8gUmV0dXJuIHRoZSBleHBvcnRzIG9mIHRoZSBtb2R1bGVcbiBcdFx0cmV0dXJuIG1vZHVsZS5leHBvcnRzO1xuIFx0fVxuXG5cbiBcdC8vIGV4cG9zZSB0aGUgbW9kdWxlcyBvYmplY3QgKF9fd2VicGFja19tb2R1bGVzX18pXG4gXHRfX3dlYnBhY2tfcmVxdWlyZV9fLm0gPSBtb2R1bGVzO1xuXG4gXHQvLyBleHBvc2UgdGhlIG1vZHVsZSBjYWNoZVxuIFx0X193ZWJwYWNrX3JlcXVpcmVfXy5jID0gaW5zdGFsbGVkTW9kdWxlcztcblxuIFx0Ly8gZGVmaW5lIGdldHRlciBmdW5jdGlvbiBmb3IgaGFybW9ueSBleHBvcnRzXG4gXHRfX3dlYnBhY2tfcmVxdWlyZV9fLmQgPSBmdW5jdGlvbihleHBvcnRzLCBuYW1lLCBnZXR0ZXIpIHtcbiBcdFx0aWYoIV9fd2VicGFja19yZXF1aXJlX18ubyhleHBvcnRzLCBuYW1lKSkge1xuIFx0XHRcdE9iamVjdC5kZWZpbmVQcm9wZXJ0eShleHBvcnRzLCBuYW1lLCB7XG4gXHRcdFx0XHRjb25maWd1cmFibGU6IGZhbHNlLFxuIFx0XHRcdFx0ZW51bWVyYWJsZTogdHJ1ZSxcbiBcdFx0XHRcdGdldDogZ2V0dGVyXG4gXHRcdFx0fSk7XG4gXHRcdH1cbiBcdH07XG5cbiBcdC8vIGdldERlZmF1bHRFeHBvcnQgZnVuY3Rpb24gZm9yIGNvbXBhdGliaWxpdHkgd2l0aCBub24taGFybW9ueSBtb2R1bGVzXG4gXHRfX3dlYnBhY2tfcmVxdWlyZV9fLm4gPSBmdW5jdGlvbihtb2R1bGUpIHtcbiBcdFx0dmFyIGdldHRlciA9IG1vZHVsZSAmJiBtb2R1bGUuX19lc01vZHVsZSA/XG4gXHRcdFx0ZnVuY3Rpb24gZ2V0RGVmYXVsdCgpIHsgcmV0dXJuIG1vZHVsZVsnZGVmYXVsdCddOyB9IDpcbiBcdFx0XHRmdW5jdGlvbiBnZXRNb2R1bGVFeHBvcnRzKCkgeyByZXR1cm4gbW9kdWxlOyB9O1xuIFx0XHRfX3dlYnBhY2tfcmVxdWlyZV9fLmQoZ2V0dGVyLCAnYScsIGdldHRlcik7XG4gXHRcdHJldHVybiBnZXR0ZXI7XG4gXHR9O1xuXG4gXHQvLyBPYmplY3QucHJvdG90eXBlLmhhc093blByb3BlcnR5LmNhbGxcbiBcdF9fd2VicGFja19yZXF1aXJlX18ubyA9IGZ1bmN0aW9uKG9iamVjdCwgcHJvcGVydHkpIHsgcmV0dXJuIE9iamVjdC5wcm90b3R5cGUuaGFzT3duUHJvcGVydHkuY2FsbChvYmplY3QsIHByb3BlcnR5KTsgfTtcblxuIFx0Ly8gX193ZWJwYWNrX3B1YmxpY19wYXRoX19cbiBcdF9fd2VicGFja19yZXF1aXJlX18ucCA9IFwiXCI7XG5cbiBcdC8vIExvYWQgZW50cnkgbW9kdWxlIGFuZCByZXR1cm4gZXhwb3J0c1xuIFx0cmV0dXJuIF9fd2VicGFja19yZXF1aXJlX18oX193ZWJwYWNrX3JlcXVpcmVfXy5zID0gMCk7XG5cblxuXG4vLyBXRUJQQUNLIEZPT1RFUiAvL1xuLy8gd2VicGFjay9ib290c3RyYXAgMzIxMmI5MzEyOTI0ZjhmOTY2MDciLCJ2YXIgbWogPSByZXF1aXJlKFwibWluaWphbnVzXCIpO1xyXG5cclxuY29uc3QgQ29udGVudEtpbmQgPSB7XHJcbiAgQXVkaW86IDEsXHJcbiAgVmlkZW86IDIsXHJcbiAgRGF0YTogNFxyXG59O1xyXG5cclxuZnVuY3Rpb24gcmFuZG9tVWludCgpIHtcclxuICByZXR1cm4gTWF0aC5mbG9vcihNYXRoLnJhbmRvbSgpICogTnVtYmVyLk1BWF9TQUZFX0lOVEVHRVIpO1xyXG59XHJcblxyXG5mdW5jdGlvbiB3YWl0Rm9yRXZlbnQodGFyZ2V0LCBldmVudCkge1xyXG4gIHJldHVybiBuZXcgUHJvbWlzZSgocmVzb2x2ZSwgcmVqZWN0KSA9PiB7XHJcbiAgICB0YXJnZXQuYWRkRXZlbnRMaXN0ZW5lcihldmVudCwgZSA9PiByZXNvbHZlKGUpLCB7IG9uY2U6IHRydWUgfSk7XHJcbiAgfSk7XHJcbn1cclxuXHJcbmFzeW5jIGZ1bmN0aW9uIGdldE1pY3JvcGhvbmUoKSB7XHJcbiAgdHJ5IHtcclxuICAgIHJldHVybiBhd2FpdCBuYXZpZ2F0b3IubWVkaWFEZXZpY2VzLmdldFVzZXJNZWRpYSh7XHJcbiAgICAgIGF1ZGlvOiB0cnVlXHJcbiAgICB9KTtcclxuICB9IGNhdGNoIChlKSB7XHJcbiAgICBpZiAoZS5uYW1lID09PSBcIk5vdEFsbG93ZWRFcnJvclwiKSB7XHJcbiAgICAgIGNvbnNvbGUud2FybihcIk1pY3JvcGhvbmUgYWNjZXNzIG5vdCBhbGxvd2VkLlwiKTtcclxuICAgIH0gZWxzZSB7XHJcbiAgICAgIGNvbnNvbGUuZXJyb3IoZSk7XHJcbiAgICB9XHJcbiAgfVxyXG59XHJcblxyXG5jb25zdCBQRUVSX0NPTk5FQ1RJT05fQ09ORklHID0ge1xyXG4gIGljZVNlcnZlcnM6IFtcclxuICAgIHsgdXJsczogXCJzdHVuOnN0dW4xLmwuZ29vZ2xlLmNvbToxOTMwMlwiIH0sXHJcbiAgICB7IHVybHM6IFwic3R1bjpzdHVuMi5sLmdvb2dsZS5jb206MTkzMDJcIiB9XHJcbiAgXVxyXG59O1xyXG5cclxuY2xhc3MgSmFudXNBZGFwdGVyIHtcclxuICBjb25zdHJ1Y3RvcigpIHtcclxuICAgIHRoaXMucm9vbSA9IG51bGw7XHJcbiAgICB0aGlzLnVzZXJJZCA9IHJhbmRvbVVpbnQoKTtcclxuXHJcbiAgICB0aGlzLnNlcnZlclVybCA9IG51bGw7XHJcbiAgICB0aGlzLndlYlJ0Y09wdGlvbnMgPSB7fTtcclxuICAgIHRoaXMud3MgPSBudWxsO1xyXG4gICAgdGhpcy5zZXNzaW9uID0gbnVsbDtcclxuXHJcbiAgICB0aGlzLnB1Ymxpc2hlciA9IG51bGw7XHJcbiAgICB0aGlzLm9jY3VwYW50cyA9IHt9O1xyXG4gICAgdGhpcy5vY2N1cGFudFByb21pc2VzID0ge307XHJcblxyXG4gICAgdGhpcy5vbldlYnNvY2tldE1lc3NhZ2UgPSB0aGlzLm9uV2Vic29ja2V0TWVzc2FnZS5iaW5kKHRoaXMpO1xyXG4gICAgdGhpcy5vbkRhdGFDaGFubmVsTWVzc2FnZSA9IHRoaXMub25EYXRhQ2hhbm5lbE1lc3NhZ2UuYmluZCh0aGlzKTtcclxuICB9XHJcblxyXG4gIHNldFNlcnZlclVybCh1cmwpIHtcclxuICAgIHRoaXMuc2VydmVyVXJsID0gdXJsO1xyXG4gIH1cclxuXHJcbiAgc2V0QXBwKGFwcCkge31cclxuXHJcbiAgc2V0Um9vbShyb29tTmFtZSkge1xyXG4gICAgdHJ5IHtcclxuICAgICAgdGhpcy5yb29tID0gcGFyc2VJbnQocm9vbU5hbWUpO1xyXG4gICAgfSBjYXRjaCAoZSkge1xyXG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXCJSb29tIG11c3QgYmUgYSBwb3NpdGl2ZSBpbnRlZ2VyLlwiKTtcclxuICAgIH1cclxuICB9XHJcblxyXG4gIHNldFdlYlJ0Y09wdGlvbnMob3B0aW9ucykge1xyXG4gICAgdGhpcy53ZWJSdGNPcHRpb25zID0gb3B0aW9ucztcclxuICB9XHJcblxyXG4gIHNldFNlcnZlckNvbm5lY3RMaXN0ZW5lcnMoc3VjY2Vzc0xpc3RlbmVyLCBmYWlsdXJlTGlzdGVuZXIpIHtcclxuICAgIHRoaXMuY29ubmVjdFN1Y2Nlc3MgPSBzdWNjZXNzTGlzdGVuZXI7XHJcbiAgICB0aGlzLmNvbm5lY3RGYWlsdXJlID0gZmFpbHVyZUxpc3RlbmVyO1xyXG4gIH1cclxuXHJcbiAgc2V0Um9vbU9jY3VwYW50TGlzdGVuZXIob2NjdXBhbnRMaXN0ZW5lcikge1xyXG4gICAgdGhpcy5vbk9jY3VwYW50c0NoYW5nZWQgPSBvY2N1cGFudExpc3RlbmVyO1xyXG4gIH1cclxuXHJcbiAgc2V0RGF0YUNoYW5uZWxMaXN0ZW5lcnMob3Blbkxpc3RlbmVyLCBjbG9zZWRMaXN0ZW5lciwgbWVzc2FnZUxpc3RlbmVyKSB7XHJcbiAgICB0aGlzLm9uT2NjdXBhbnRDb25uZWN0ZWQgPSBvcGVuTGlzdGVuZXI7XHJcbiAgICB0aGlzLm9uT2NjdXBhbnREaXNjb25uZWN0ZWQgPSBjbG9zZWRMaXN0ZW5lcjtcclxuICAgIHRoaXMub25PY2N1cGFudE1lc3NhZ2UgPSBtZXNzYWdlTGlzdGVuZXI7XHJcbiAgfVxyXG5cclxuICBjb25uZWN0KCkge1xyXG4gICAgdGhpcy53cyA9IG5ldyBXZWJTb2NrZXQodGhpcy5zZXJ2ZXJVcmwsIFwiamFudXMtcHJvdG9jb2xcIik7XHJcbiAgICB0aGlzLnNlc3Npb24gPSBuZXcgbWouSmFudXNTZXNzaW9uKHRoaXMud3Muc2VuZC5iaW5kKHRoaXMud3MpKTtcclxuICAgIHRoaXMud3MuYWRkRXZlbnRMaXN0ZW5lcihcIm9wZW5cIiwgXyA9PiB0aGlzLm9uV2Vic29ja2V0T3BlbigpKTtcclxuICAgIHRoaXMud3MuYWRkRXZlbnRMaXN0ZW5lcihcIm1lc3NhZ2VcIiwgdGhpcy5vbldlYnNvY2tldE1lc3NhZ2UpO1xyXG4gIH1cclxuXHJcbiAgYXN5bmMgb25XZWJzb2NrZXRPcGVuKCkge1xyXG4gICAgLy8gQ3JlYXRlIHRoZSBKYW51cyBTZXNzaW9uXHJcbiAgICBhd2FpdCB0aGlzLnNlc3Npb24uY3JlYXRlKCk7XHJcblxyXG4gICAgLy8gQXR0YWNoIHRoZSBTRlUgUGx1Z2luIGFuZCBjcmVhdGUgYSBSVENQZWVyQ29ubmVjdGlvbiBmb3IgdGhlIHB1Ymxpc2hlci5cclxuICAgIC8vIFRoZSBwdWJsaXNoZXIgc2VuZHMgYXVkaW8gYW5kIG9wZW5zIHR3byBiaWRpcmVjdGlvbmFsIGRhdGEgY2hhbm5lbHMuXHJcbiAgICAvLyBPbmUgcmVsaWFibGUgZGF0YWNoYW5uZWwgYW5kIG9uZSB1bnJlbGlhYmxlLlxyXG4gICAgdmFyIHB1Ymxpc2hlclByb21pc2UgPSB0aGlzLmNyZWF0ZVB1Ymxpc2hlcigpO1xyXG4gICAgdGhpcy5vY2N1cGFudFByb21pc2VzW3RoaXMudXNlcklkXSA9IHB1Ymxpc2hlclByb21pc2U7XHJcbiAgICB0aGlzLnB1Ymxpc2hlciA9IGF3YWl0IHB1Ymxpc2hlclByb21pc2U7XHJcblxyXG4gICAgdGhpcy5jb25uZWN0U3VjY2Vzcyh0aGlzLnVzZXJJZCk7XHJcblxyXG4gICAgLy8gQWRkIGFsbCBvZiB0aGUgaW5pdGlhbCBvY2N1cGFudHMuXHJcbiAgICBmb3IgKGxldCBvY2N1cGFudElkIG9mIHRoaXMucHVibGlzaGVyLmluaXRpYWxPY2N1cGFudHMpIHtcclxuICAgICAgaWYgKG9jY3VwYW50SWQgIT09IHRoaXMudXNlcklkKSB7XHJcbiAgICAgICAgdGhpcy5vY2N1cGFudFByb21pc2VzW29jY3VwYW50SWRdID0gdGhpcy5hZGRPY2N1cGFudChvY2N1cGFudElkKTtcclxuICAgICAgfVxyXG4gICAgfVxyXG4gIH1cclxuXHJcbiAgb25XZWJzb2NrZXRNZXNzYWdlKGV2ZW50KSB7XHJcbiAgICB2YXIgbWVzc2FnZSA9IEpTT04ucGFyc2UoZXZlbnQuZGF0YSk7XHJcbiAgICB0aGlzLnNlc3Npb24ucmVjZWl2ZShtZXNzYWdlKTtcclxuXHJcbiAgICAvLyBIYW5kbGUgYWxsIG9mIHRoZSBqb2luIGFuZCBsZWF2ZSBldmVudHMgZnJvbSB0aGUgcHVibGlzaGVyLlxyXG4gICAgaWYgKG1lc3NhZ2UucGx1Z2luZGF0YSAmJiBtZXNzYWdlLnBsdWdpbmRhdGEuZGF0YSkge1xyXG4gICAgICB2YXIgZGF0YSA9IG1lc3NhZ2UucGx1Z2luZGF0YS5kYXRhO1xyXG5cclxuICAgICAgaWYgKGRhdGEuZXZlbnQgPT09IFwiam9pblwiKSB7XHJcbiAgICAgICAgdGhpcy5vY2N1cGFudFByb21pc2VzW2RhdGEudXNlcl9pZF0gPSB0aGlzLmFkZE9jY3VwYW50KGRhdGEudXNlcl9pZCk7XHJcbiAgICAgIH0gZWxzZSBpZiAoZGF0YS5ldmVudCAmJiBkYXRhLmV2ZW50ID09PSBcImxlYXZlXCIpIHtcclxuICAgICAgICB0aGlzLnJlbW92ZU9jY3VwYW50KGRhdGEudXNlcl9pZCk7XHJcbiAgICAgIH1cclxuICAgIH1cclxuICB9XHJcblxyXG4gIGFzeW5jIGFkZE9jY3VwYW50KG9jY3VwYW50SWQpIHtcclxuICAgIHZhciBzdWJzY3JpYmVyID0gYXdhaXQgdGhpcy5jcmVhdGVTdWJzY3JpYmVyKG9jY3VwYW50SWQpO1xyXG4gICAgLy8gQ2FsbCB0aGUgTmV0d29ya2VkIEFGcmFtZSBjYWxsYmFja3MgZm9yIHRoZSBuZXcgb2NjdXBhbnQuXHJcbiAgICB0aGlzLm9uT2NjdXBhbnRDb25uZWN0ZWQob2NjdXBhbnRJZCk7XHJcbiAgICB0aGlzLm9jY3VwYW50c1tvY2N1cGFudElkXSA9IHRydWU7XHJcbiAgICB0aGlzLm9uT2NjdXBhbnRzQ2hhbmdlZCh0aGlzLm9jY3VwYW50cyk7XHJcbiAgICByZXR1cm4gc3Vic2NyaWJlcjtcclxuICB9XHJcblxyXG4gIHJlbW92ZU9jY3VwYW50KG9jY3VwYW50SWQpIHtcclxuICAgIGlmICh0aGlzLm9jY3VwYW50c1tvY2N1cGFudElkXSkge1xyXG4gICAgICBkZWxldGUgdGhpcy5vY2N1cGFudHNbb2NjdXBhbnRJZF07XHJcbiAgICAgIC8vIENhbGwgdGhlIE5ldHdvcmtlZCBBRnJhbWUgY2FsbGJhY2tzIGZvciB0aGUgcmVtb3ZlZCBvY2N1cGFudC5cclxuICAgICAgdGhpcy5vbk9jY3VwYW50RGlzY29ubmVjdGVkKG9jY3VwYW50SWQpO1xyXG4gICAgICB0aGlzLm9uT2NjdXBhbnRzQ2hhbmdlZCh0aGlzLm9jY3VwYW50cyk7XHJcbiAgICB9XHJcbiAgfVxyXG5cclxuICBhc3luYyBjcmVhdGVQdWJsaXNoZXIoKSB7XHJcbiAgICB2YXIgaGFuZGxlID0gbmV3IG1qLkphbnVzUGx1Z2luSGFuZGxlKHRoaXMuc2Vzc2lvbik7XHJcbiAgICBhd2FpdCBoYW5kbGUuYXR0YWNoKFwiamFudXMucGx1Z2luLnNmdVwiKTtcclxuXHJcbiAgICB2YXIgcGVlckNvbm5lY3Rpb24gPSBuZXcgUlRDUGVlckNvbm5lY3Rpb24oUEVFUl9DT05ORUNUSU9OX0NPTkZJRyk7XHJcblxyXG4gICAgcGVlckNvbm5lY3Rpb24uYWRkRXZlbnRMaXN0ZW5lcihcImljZWNhbmRpZGF0ZVwiLCBldmVudCA9PiB7XHJcbiAgICAgIGhhbmRsZS5zZW5kVHJpY2tsZShldmVudC5jYW5kaWRhdGUpO1xyXG4gICAgfSk7XHJcblxyXG4gICAgLy8gQ3JlYXRlIGFuIHVucmVsaWFibGUgZGF0YWNoYW5uZWwgZm9yIHNlbmRpbmcgYW5kIHJlY2VpdmluZyBjb21wb25lbnQgdXBkYXRlcywgZXRjLlxyXG4gICAgdmFyIHVucmVsaWFibGVDaGFubmVsID0gcGVlckNvbm5lY3Rpb24uY3JlYXRlRGF0YUNoYW5uZWwoXCJ1bnJlbGlhYmxlXCIsIHtcclxuICAgICAgb3JkZXJlZDogZmFsc2UsXHJcbiAgICAgIG1heFJldHJhbnNtaXRzOiAwXHJcbiAgICB9KTtcclxuICAgIHVucmVsaWFibGVDaGFubmVsLmFkZEV2ZW50TGlzdGVuZXIoXCJtZXNzYWdlXCIsIHRoaXMub25EYXRhQ2hhbm5lbE1lc3NhZ2UpO1xyXG5cclxuICAgIC8vIENyZWF0ZSBhIHJlbGlhYmxlIGRhdGFjaGFubmVsIGZvciBzZW5kaW5nIGFuZCByZWNpZXZpbmcgZW50aXR5IGluc3RhbnRpYXRpb25zLCBldGMuXHJcbiAgICB2YXIgcmVsaWFibGVDaGFubmVsID0gcGVlckNvbm5lY3Rpb24uY3JlYXRlRGF0YUNoYW5uZWwoXCJyZWxpYWJsZVwiLCB7XHJcbiAgICAgIG9yZGVyZWQ6IHRydWVcclxuICAgIH0pO1xyXG4gICAgcmVsaWFibGVDaGFubmVsLmFkZEV2ZW50TGlzdGVuZXIoXCJtZXNzYWdlXCIsIHRoaXMub25EYXRhQ2hhbm5lbE1lc3NhZ2UpO1xyXG5cclxuICAgIHZhciBtZWRpYVN0cmVhbTtcclxuICAgIGlmICh0aGlzLndlYlJ0Y09wdGlvbnMuYXVkaW8pIHtcclxuICAgICAgbWVkaWFTdHJlYW0gPSBhd2FpdCBnZXRNaWNyb3Bob25lKCk7XHJcblxyXG4gICAgICBpZiAobWVkaWFTdHJlYW0pIHtcclxuICAgICAgICBwZWVyQ29ubmVjdGlvbi5hZGRTdHJlYW0obWVkaWFTdHJlYW0pO1xyXG4gICAgICB9XHJcbiAgICB9XHJcblxyXG4gICAgdmFyIG9mZmVyID0gYXdhaXQgcGVlckNvbm5lY3Rpb24uY3JlYXRlT2ZmZXIoKTtcclxuICAgIGF3YWl0IHBlZXJDb25uZWN0aW9uLnNldExvY2FsRGVzY3JpcHRpb24ob2ZmZXIpO1xyXG5cclxuICAgIHZhciBhbnN3ZXIgPSBhd2FpdCBoYW5kbGUuc2VuZEpzZXAob2ZmZXIpO1xyXG4gICAgYXdhaXQgcGVlckNvbm5lY3Rpb24uc2V0UmVtb3RlRGVzY3JpcHRpb24oYW5zd2VyLmpzZXApO1xyXG5cclxuICAgIC8vIFdhaXQgZm9yIHRoZSByZWxpYWJsZSBkYXRhY2hhbm5lbCB0byBiZSBvcGVuIGJlZm9yZSB3ZSBzdGFydCBzZW5kaW5nIG1lc3NhZ2VzIG9uIGl0LlxyXG4gICAgYXdhaXQgd2FpdEZvckV2ZW50KHJlbGlhYmxlQ2hhbm5lbCwgXCJvcGVuXCIpO1xyXG5cclxuICAgIC8vIFNlbmQgam9pbiBtZXNzYWdlIHRvIGphbnVzLiBMaXN0ZW4gZm9yIGpvaW4vbGVhdmUgbWVzc2FnZXMuIEF1dG9tYXRpY2FsbHkgc3Vic2NyaWJlIHRvIGFsbCB1c2VycycgV2ViUlRDIGRhdGEuXHJcbiAgICB2YXIgbWVzc2FnZSA9IGF3YWl0IHRoaXMuc2VuZEpvaW4oaGFuZGxlLCB0aGlzLnJvb20sIHRoaXMudXNlcklkLCB0cnVlKTtcclxuXHJcbiAgICB2YXIgaW5pdGlhbE9jY3VwYW50cyA9IG1lc3NhZ2UucGx1Z2luZGF0YS5kYXRhLnJlc3BvbnNlLnVzZXJfaWRzO1xyXG5cclxuICAgIHJldHVybiB7XHJcbiAgICAgIGhhbmRsZSxcclxuICAgICAgaW5pdGlhbE9jY3VwYW50cyxcclxuICAgICAgcmVsaWFibGVDaGFubmVsLFxyXG4gICAgICB1bnJlbGlhYmxlQ2hhbm5lbCxcclxuICAgICAgbWVkaWFTdHJlYW0sXHJcbiAgICAgIHBlZXJDb25uZWN0aW9uXHJcbiAgICB9O1xyXG4gIH1cclxuXHJcbiAgYXN5bmMgY3JlYXRlU3Vic2NyaWJlcihvY2N1cGFudElkKSB7XHJcbiAgICB2YXIgaGFuZGxlID0gbmV3IG1qLkphbnVzUGx1Z2luSGFuZGxlKHRoaXMuc2Vzc2lvbik7XHJcbiAgICBhd2FpdCBoYW5kbGUuYXR0YWNoKFwiamFudXMucGx1Z2luLnNmdVwiKTtcclxuXHJcbiAgICB2YXIgcGVlckNvbm5lY3Rpb24gPSBuZXcgUlRDUGVlckNvbm5lY3Rpb24oUEVFUl9DT05ORUNUSU9OX0NPTkZJRyk7XHJcblxyXG4gICAgcGVlckNvbm5lY3Rpb24uYWRkRXZlbnRMaXN0ZW5lcihcImljZWNhbmRpZGF0ZVwiLCBldmVudCA9PiB7XHJcbiAgICAgIGhhbmRsZS5zZW5kVHJpY2tsZShldmVudC5jYW5kaWRhdGUpO1xyXG4gICAgfSk7XHJcblxyXG4gICAgdmFyIG9mZmVyID0gYXdhaXQgcGVlckNvbm5lY3Rpb24uY3JlYXRlT2ZmZXIoe1xyXG4gICAgICBvZmZlclRvUmVjZWl2ZUF1ZGlvOiB0cnVlXHJcbiAgICB9KTtcclxuXHJcbiAgICBhd2FpdCBwZWVyQ29ubmVjdGlvbi5zZXRMb2NhbERlc2NyaXB0aW9uKG9mZmVyKTtcclxuICAgIHZhciBhbnN3ZXIgPSBhd2FpdCBoYW5kbGUuc2VuZEpzZXAob2ZmZXIpO1xyXG4gICAgYXdhaXQgcGVlckNvbm5lY3Rpb24uc2V0UmVtb3RlRGVzY3JpcHRpb24oYW5zd2VyLmpzZXApO1xyXG5cclxuICAgIC8vIFNlbmQgam9pbiBtZXNzYWdlIHRvIGphbnVzLiBEb24ndCBsaXN0ZW4gZm9yIGpvaW4vbGVhdmUgbWVzc2FnZXMuIFN1YnNjcmliZSB0byB0aGUgb2NjdXBhbnQncyBhdWRpbyBzdHJlYW0uXHJcbiAgICBhd2FpdCB0aGlzLnNlbmRKb2luKGhhbmRsZSwgdGhpcy5yb29tLCB0aGlzLnVzZXJJZCwgZmFsc2UsIFtcclxuICAgICAge1xyXG4gICAgICAgIHB1Ymxpc2hlcl9pZDogb2NjdXBhbnRJZCxcclxuICAgICAgICBjb250ZW50X2tpbmQ6IENvbnRlbnRLaW5kLkF1ZGlvXHJcbiAgICAgIH1cclxuICAgIF0pO1xyXG5cclxuICAgIC8vIEdldCB0aGUgb2NjdXBhbnQncyBhdWRpbyBzdHJlYW0uXHJcbiAgICB2YXIgc3RyZWFtcyA9IHBlZXJDb25uZWN0aW9uLmdldFJlbW90ZVN0cmVhbXMoKTtcclxuICAgIHZhciBtZWRpYVN0cmVhbSA9IHN0cmVhbXMubGVuZ3RoID4gMCA/IHN0cmVhbXNbMF0gOiBudWxsO1xyXG5cclxuICAgIHJldHVybiB7XHJcbiAgICAgIGhhbmRsZSxcclxuICAgICAgbWVkaWFTdHJlYW0sXHJcbiAgICAgIHBlZXJDb25uZWN0aW9uXHJcbiAgICB9O1xyXG4gIH1cclxuXHJcbiAgc2VuZEpvaW4oaGFuZGxlLCByb29tSWQsIHVzZXJJZCwgbm90aWZ5LCBzcGVjcykge1xyXG4gICAgcmV0dXJuIGhhbmRsZS5zZW5kTWVzc2FnZSh7XHJcbiAgICAgIGtpbmQ6IFwiam9pblwiLFxyXG4gICAgICByb29tX2lkOiByb29tSWQsXHJcbiAgICAgIHVzZXJfaWQ6IHVzZXJJZCxcclxuICAgICAgbm90aWZ5LFxyXG4gICAgICBzdWJzY3JpcHRpb25fc3BlY3M6IHNwZWNzXHJcbiAgICB9KTtcclxuICB9XHJcblxyXG4gIG9uRGF0YUNoYW5uZWxNZXNzYWdlKGV2ZW50KSB7XHJcbiAgICB2YXIgbWVzc2FnZSA9IEpTT04ucGFyc2UoZXZlbnQuZGF0YSk7XHJcblxyXG4gICAgaWYgKG1lc3NhZ2UuZGF0YVR5cGUpIHtcclxuICAgICAgdGhpcy5vbk9jY3VwYW50TWVzc2FnZShudWxsLCBtZXNzYWdlLmRhdGFUeXBlLCBtZXNzYWdlLmRhdGEpO1xyXG4gICAgfVxyXG4gIH1cclxuXHJcbiAgc2hvdWxkU3RhcnRDb25uZWN0aW9uVG8oY2xpZW50SWQpIHtcclxuICAgIHJldHVybiB0cnVlO1xyXG4gIH1cclxuXHJcbiAgc3RhcnRTdHJlYW1Db25uZWN0aW9uKGNsaWVudElkKSB7fVxyXG5cclxuICBjbG9zZVN0cmVhbUNvbm5lY3Rpb24oY2xpZW50SWQpIHt9XHJcblxyXG4gIGdldENvbm5lY3RTdGF0dXMoY2xpZW50SWQpIHtcclxuICAgIGlmICh0aGlzLm9jY3VwYW50c1tjbGllbnRJZF0pIHtcclxuICAgICAgcmV0dXJuIE5BRi5hZGFwdGVycy5JU19DT05ORUNURUQ7XHJcbiAgICB9IGVsc2Uge1xyXG4gICAgICByZXR1cm4gTkFGLmFkYXB0ZXJzLk5PVF9DT05ORUNURUQ7XHJcbiAgICB9XHJcbiAgfVxyXG5cclxuICBnZXRNZWRpYVN0cmVhbShjbGllbnRJZCkge1xyXG4gICAgdmFyIG9jY3VwYW50UHJvbWlzZSA9IHRoaXMub2NjdXBhbnRQcm9taXNlc1tjbGllbnRJZF07XHJcblxyXG4gICAgaWYgKCFvY2N1cGFudFByb21pc2UpIHtcclxuICAgICAgcmV0dXJuIFByb21pc2UucmVqZWN0KFxyXG4gICAgICAgIG5ldyBFcnJvcihgU3Vic2NyaWJlciBmb3IgY2xpZW50OiAke2NsaWVudElkfSBkb2VzIG5vdCBleGlzdC5gKVxyXG4gICAgICApO1xyXG4gICAgfVxyXG5cclxuICAgIHJldHVybiBvY2N1cGFudFByb21pc2UudGhlbihzID0+IHMubWVkaWFTdHJlYW0pO1xyXG4gIH1cclxuXHJcbiAgZW5hYmxlTWljcm9waG9uZShlbmFibGVkKSB7XHJcbiAgICBpZiAodGhpcy5wdWJsaXNoZXIgJiYgdGhpcy5wdWJsaXNoZXIubWVkaWFTdHJlYW0pIHtcclxuICAgICAgdmFyIGF1ZGlvVHJhY2tzID0gdGhpcy5wdWJsaXNoZXIubWVkaWFTdHJlYW0uZ2V0QXVkaW9UcmFja3MoKTtcclxuXHJcbiAgICAgIGlmIChhdWRpb1RyYWNrcy5sZW5ndGggPiAwKSB7XHJcbiAgICAgICAgYXVkaW9UcmFja3NbMF0uZW5hYmxlZCA9IGVuYWJsZWQ7XHJcbiAgICAgIH1cclxuICAgIH1cclxuICB9XHJcblxyXG4gIHNlbmREYXRhKGNsaWVudElkLCBkYXRhVHlwZSwgZGF0YSkge1xyXG4gICAgdGhpcy5wdWJsaXNoZXIudW5yZWxpYWJsZUNoYW5uZWwuc2VuZChcclxuICAgICAgSlNPTi5zdHJpbmdpZnkoeyBjbGllbnRJZCwgZGF0YVR5cGUsIGRhdGEgfSlcclxuICAgICk7XHJcbiAgfVxyXG5cclxuICBzZW5kRGF0YUd1YXJhbnRlZWQoY2xpZW50SWQsIGRhdGFUeXBlLCBkYXRhKSB7XHJcbiAgICB0aGlzLnB1Ymxpc2hlci5yZWxpYWJsZUNoYW5uZWwuc2VuZChcclxuICAgICAgSlNPTi5zdHJpbmdpZnkoeyBjbGllbnRJZCwgZGF0YVR5cGUsIGRhdGEgfSlcclxuICAgICk7XHJcbiAgfVxyXG5cclxuICBicm9hZGNhc3REYXRhKGRhdGFUeXBlLCBkYXRhKSB7XHJcbiAgICB0aGlzLnB1Ymxpc2hlci51bnJlbGlhYmxlQ2hhbm5lbC5zZW5kKEpTT04uc3RyaW5naWZ5KHsgZGF0YVR5cGUsIGRhdGEgfSkpO1xyXG4gIH1cclxuXHJcbiAgYnJvYWRjYXN0RGF0YUd1YXJhbnRlZWQoZGF0YVR5cGUsIGRhdGEpIHtcclxuICAgIHRoaXMucHVibGlzaGVyLnJlbGlhYmxlQ2hhbm5lbC5zZW5kKEpTT04uc3RyaW5naWZ5KHsgZGF0YVR5cGUsIGRhdGEgfSkpO1xyXG4gIH1cclxufVxyXG5cclxuTkFGLmFkYXB0ZXJzLnJlZ2lzdGVyKFwiamFudXNcIiwgSmFudXNBZGFwdGVyKTtcclxuXHJcbm1vZHVsZS5leHBvcnRzID0gSmFudXNBZGFwdGVyO1xyXG5cblxuXG4vLyBXRUJQQUNLIEZPT1RFUiAvL1xuLy8gLi9zcmMvaW5kZXguanMiLCIvKiogV2hldGhlciB0byBsb2cgaW5mb3JtYXRpb24gYWJvdXQgaW5jb21pbmcgYW5kIG91dGdvaW5nIEphbnVzIHNpZ25hbHMuICoqL1xudmFyIHZlcmJvc2UgPSBmYWxzZTtcblxuLyoqXG4gKiBSZXByZXNlbnRzIGEgaGFuZGxlIHRvIGEgc2luZ2xlIEphbnVzIHBsdWdpbiBvbiBhIEphbnVzIHNlc3Npb24uIEVhY2ggV2ViUlRDIGNvbm5lY3Rpb24gdG8gdGhlIEphbnVzIHNlcnZlciB3aWxsIGJlXG4gKiBhc3NvY2lhdGVkIHdpdGggYSBzaW5nbGUgaGFuZGxlLiBPbmNlIGF0dGFjaGVkIHRvIHRoZSBzZXJ2ZXIsIHRoaXMgaGFuZGxlIHdpbGwgYmUgZ2l2ZW4gYSB1bmlxdWUgSUQgd2hpY2ggc2hvdWxkIGJlXG4gKiB1c2VkIHRvIGFzc29jaWF0ZSBpdCB3aXRoIGZ1dHVyZSBzaWduYWxsaW5nIG1lc3NhZ2VzLlxuICpcbiAqIFNlZSBodHRwczovL2phbnVzLmNvbmYubWVldGVjaG8uY29tL2RvY3MvcmVzdC5odG1sI2hhbmRsZXMuXG4gKiovXG5mdW5jdGlvbiBKYW51c1BsdWdpbkhhbmRsZShzZXNzaW9uKSB7XG4gIHRoaXMuc2Vzc2lvbiA9IHNlc3Npb247XG4gIHRoaXMuaWQgPSB1bmRlZmluZWQ7XG59XG5cbi8qKiBBdHRhY2hlcyB0aGlzIGhhbmRsZSB0byB0aGUgSmFudXMgc2VydmVyIGFuZCBzZXRzIGl0cyBJRC4gKiovXG5KYW51c1BsdWdpbkhhbmRsZS5wcm90b3R5cGUuYXR0YWNoID0gZnVuY3Rpb24ocGx1Z2luKSB7XG4gIHZhciBwYXlsb2FkID0geyBqYW51czogXCJhdHRhY2hcIiwgcGx1Z2luOiBwbHVnaW4sIFwiZm9yY2UtYnVuZGxlXCI6IHRydWUsIFwiZm9yY2UtcnRjcC1tdXhcIjogdHJ1ZSB9O1xuICByZXR1cm4gdGhpcy5zZXNzaW9uLnNlbmQocGF5bG9hZCkudGhlbihyZXNwID0+IHtcbiAgICB0aGlzLmlkID0gcmVzcC5kYXRhLmlkO1xuICAgIHJldHVybiByZXNwO1xuICB9KTtcbn07XG5cbi8qKiBEZXRhY2hlcyB0aGlzIGhhbmRsZS4gKiovXG5KYW51c1BsdWdpbkhhbmRsZS5wcm90b3R5cGUuZGV0YWNoID0gZnVuY3Rpb24oKSB7XG4gIHJldHVybiB0aGlzLnNlbmQoeyBqYW51czogXCJkZXRhY2hcIiB9KTtcbn07XG5cbi8qKlxuICogU2VuZHMgYSBzaWduYWwgYXNzb2NpYXRlZCB3aXRoIHRoaXMgaGFuZGxlLiBTaWduYWxzIHNob3VsZCBiZSBKU09OLXNlcmlhbGl6YWJsZSBvYmplY3RzLiBSZXR1cm5zIGEgcHJvbWlzZSB0aGF0IHdpbGxcbiAqIGJlIHJlc29sdmVkIG9yIHJlamVjdGVkIHdoZW4gYSByZXNwb25zZSB0byB0aGlzIHNpZ25hbCBpcyByZWNlaXZlZCwgb3Igd2hlbiBubyByZXNwb25zZSBpcyByZWNlaXZlZCB3aXRoaW4gdGhlXG4gKiBzZXNzaW9uIHRpbWVvdXQuXG4gKiovXG5KYW51c1BsdWdpbkhhbmRsZS5wcm90b3R5cGUuc2VuZCA9IGZ1bmN0aW9uKHNpZ25hbCkge1xuICByZXR1cm4gdGhpcy5zZXNzaW9uLnNlbmQoT2JqZWN0LmFzc2lnbih7IGhhbmRsZV9pZDogdGhpcy5pZCB9LCBzaWduYWwpKTtcbn07XG5cbi8qKiBTZW5kcyBhIHBsdWdpbi1zcGVjaWZpYyBtZXNzYWdlIGFzc29jaWF0ZWQgd2l0aCB0aGlzIGhhbmRsZS4gKiovXG5KYW51c1BsdWdpbkhhbmRsZS5wcm90b3R5cGUuc2VuZE1lc3NhZ2UgPSBmdW5jdGlvbihib2R5KSB7XG4gIHJldHVybiB0aGlzLnNlbmQoeyBqYW51czogXCJtZXNzYWdlXCIsIGJvZHk6IGJvZHkgfSk7XG59O1xuXG4vKiogU2VuZHMgYSBKU0VQIG9mZmVyIG9yIGFuc3dlciBhc3NvY2lhdGVkIHdpdGggdGhpcyBoYW5kbGUuICoqL1xuSmFudXNQbHVnaW5IYW5kbGUucHJvdG90eXBlLnNlbmRKc2VwID0gZnVuY3Rpb24oanNlcCkge1xuICByZXR1cm4gdGhpcy5zZW5kKHsgamFudXM6IFwibWVzc2FnZVwiLCBib2R5OiB7fSwganNlcDoganNlcCB9KTtcbn07XG5cbi8qKiBTZW5kcyBhbiBJQ0UgdHJpY2tsZSBjYW5kaWRhdGUgYXNzb2NpYXRlZCB3aXRoIHRoaXMgaGFuZGxlLiAqKi9cbkphbnVzUGx1Z2luSGFuZGxlLnByb3RvdHlwZS5zZW5kVHJpY2tsZSA9IGZ1bmN0aW9uKGNhbmRpZGF0ZSkge1xuICByZXR1cm4gdGhpcy5zZW5kKHsgamFudXM6IFwidHJpY2tsZVwiLCAgY2FuZGlkYXRlOiBjYW5kaWRhdGUgfSk7XG59O1xuXG4vKipcbiAqIFJlcHJlc2VudHMgYSBKYW51cyBzZXNzaW9uIC0tIGEgSmFudXMgY29udGV4dCBmcm9tIHdpdGhpbiB3aGljaCB5b3UgY2FuIG9wZW4gbXVsdGlwbGUgaGFuZGxlcyBhbmQgY29ubmVjdGlvbnMuIE9uY2VcbiAqIGNyZWF0ZWQsIHRoaXMgc2Vzc2lvbiB3aWxsIGJlIGdpdmVuIGEgdW5pcXVlIElEIHdoaWNoIHNob3VsZCBiZSB1c2VkIHRvIGFzc29jaWF0ZSBpdCB3aXRoIGZ1dHVyZSBzaWduYWxsaW5nIG1lc3NhZ2VzLlxuICpcbiAqIFNlZSBodHRwczovL2phbnVzLmNvbmYubWVldGVjaG8uY29tL2RvY3MvcmVzdC5odG1sI3Nlc3Npb25zLlxuICoqL1xuZnVuY3Rpb24gSmFudXNTZXNzaW9uKG91dHB1dCwgb3B0aW9ucykge1xuICB0aGlzLm91dHB1dCA9IG91dHB1dDtcbiAgdGhpcy5pZCA9IHVuZGVmaW5lZDtcbiAgdGhpcy5uZXh0VHhJZCA9IDA7XG4gIHRoaXMudHhucyA9IHt9O1xuICB0aGlzLm9wdGlvbnMgPSBvcHRpb25zIHx8IHtcbiAgICB0aW1lb3V0TXM6IDEwMDAwLFxuICAgIGtlZXBhbGl2ZU1zOiAzMDAwMFxuICB9O1xufVxuXG4vKiogQ3JlYXRlcyB0aGlzIHNlc3Npb24gb24gdGhlIEphbnVzIHNlcnZlciBhbmQgc2V0cyBpdHMgSUQuICoqL1xuSmFudXNTZXNzaW9uLnByb3RvdHlwZS5jcmVhdGUgPSBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIHRoaXMuc2VuZCh7IGphbnVzOiBcImNyZWF0ZVwiIH0pLnRoZW4ocmVzcCA9PiB7XG4gICAgdGhpcy5pZCA9IHJlc3AuZGF0YS5pZDtcbiAgICByZXR1cm4gcmVzcDtcbiAgfSk7XG59O1xuXG4vKiogRGVzdHJveXMgdGhpcyBzZXNzaW9uLiAqKi9cbkphbnVzU2Vzc2lvbi5wcm90b3R5cGUuZGVzdHJveSA9IGZ1bmN0aW9uKCkge1xuICByZXR1cm4gdGhpcy5zZW5kKHsgamFudXM6IFwiZGVzdHJveVwiIH0pO1xufTtcblxuLyoqXG4gKiBDYWxsYmFjayBmb3IgcmVjZWl2aW5nIEpTT04gc2lnbmFsbGluZyBtZXNzYWdlcyBwZXJ0aW5lbnQgdG8gdGhpcyBzZXNzaW9uLiBJZiB0aGUgc2lnbmFscyBhcmUgcmVzcG9uc2VzIHRvIHByZXZpb3VzbHlcbiAqIHNlbnQgc2lnbmFscywgdGhlIHByb21pc2VzIGZvciB0aGUgb3V0Z29pbmcgc2lnbmFscyB3aWxsIGJlIHJlc29sdmVkIG9yIHJlamVjdGVkIGFwcHJvcHJpYXRlbHkgd2l0aCB0aGlzIHNpZ25hbCBhcyBhblxuICogYXJndW1lbnQuXG4gKlxuICogRXh0ZXJuYWwgY2FsbGVycyBzaG91bGQgY2FsbCB0aGlzIGZ1bmN0aW9uIGV2ZXJ5IHRpbWUgYSBuZXcgc2lnbmFsIGFycml2ZXMgb24gdGhlIHRyYW5zcG9ydDsgZm9yIGV4YW1wbGUsIGluIGFcbiAqIFdlYlNvY2tldCdzIGBtZXNzYWdlYCBldmVudCwgb3Igd2hlbiBhIG5ldyBkYXR1bSBzaG93cyB1cCBpbiBhbiBIVFRQIGxvbmctcG9sbGluZyByZXNwb25zZS5cbiAqKi9cbkphbnVzU2Vzc2lvbi5wcm90b3R5cGUucmVjZWl2ZSA9IGZ1bmN0aW9uKHNpZ25hbCkge1xuICBpZiAobW9kdWxlLmV4cG9ydHMudmVyYm9zZSkge1xuICAgIGNvbnNvbGUuZGVidWcoXCJJbmNvbWluZyBKYW51cyBzaWduYWw6IFwiLCBzaWduYWwpO1xuICB9XG4gIGlmIChzaWduYWwudHJhbnNhY3Rpb24gIT0gbnVsbCkge1xuICAgIHZhciBoYW5kbGVycyA9IHRoaXMudHhuc1tzaWduYWwudHJhbnNhY3Rpb25dO1xuICAgIGlmIChzaWduYWwuamFudXMgPT09IFwiYWNrXCIgJiYgc2lnbmFsLmhpbnQpIHtcbiAgICAgIC8vIHRoaXMgaXMgYW4gYWNrIG9mIGFuIGFzeW5jaHJvbm91c2x5LXByb2Nlc3NlZCByZXF1ZXN0LCB3ZSBzaG91bGQgd2FpdFxuICAgICAgLy8gdG8gcmVzb2x2ZSB0aGUgcHJvbWlzZSB1bnRpbCB0aGUgYWN0dWFsIHJlc3BvbnNlIGNvbWVzIGluXG4gICAgfSBlbHNlIGlmIChoYW5kbGVycyAhPSBudWxsKSB7XG4gICAgICBpZiAoaGFuZGxlcnMudGltZW91dCAhPSBudWxsKSB7XG4gICAgICAgIGNsZWFyVGltZW91dChoYW5kbGVycy50aW1lb3V0KTtcbiAgICAgIH1cbiAgICAgIGRlbGV0ZSB0aGlzLnR4bnNbc2lnbmFsLnRyYW5zYWN0aW9uXTtcbiAgICAgIChzaWduYWwuamFudXMgPT09IFwiZXJyb3JcIiA/IGhhbmRsZXJzLnJlamVjdCA6IGhhbmRsZXJzLnJlc29sdmUpKHNpZ25hbCk7XG4gICAgfVxuICB9XG59O1xuXG4vKipcbiAqIFNlbmRzIGEgc2lnbmFsIGFzc29jaWF0ZWQgd2l0aCB0aGlzIHNlc3Npb24uIFNpZ25hbHMgc2hvdWxkIGJlIEpTT04tc2VyaWFsaXphYmxlIG9iamVjdHMuIFJldHVybnMgYSBwcm9taXNlIHRoYXQgd2lsbFxuICogYmUgcmVzb2x2ZWQgb3IgcmVqZWN0ZWQgd2hlbiBhIHJlc3BvbnNlIHRvIHRoaXMgc2lnbmFsIGlzIHJlY2VpdmVkLCBvciB3aGVuIG5vIHJlc3BvbnNlIGlzIHJlY2VpdmVkIHdpdGhpbiB0aGVcbiAqIHNlc3Npb24gdGltZW91dC5cbiAqKi9cbkphbnVzU2Vzc2lvbi5wcm90b3R5cGUuc2VuZCA9IGZ1bmN0aW9uKHNpZ25hbCkge1xuICBpZiAobW9kdWxlLmV4cG9ydHMudmVyYm9zZSkge1xuICAgIGNvbnNvbGUuZGVidWcoXCJPdXRnb2luZyBKYW51cyBzaWduYWw6IFwiLCBzaWduYWwpO1xuICB9XG4gIHNpZ25hbCA9IE9iamVjdC5hc3NpZ24oe1xuICAgIHNlc3Npb25faWQ6IHRoaXMuaWQsXG4gICAgdHJhbnNhY3Rpb246ICh0aGlzLm5leHRUeElkKyspLnRvU3RyaW5nKClcbiAgfSwgc2lnbmFsKTtcbiAgcmV0dXJuIG5ldyBQcm9taXNlKChyZXNvbHZlLCByZWplY3QpID0+IHtcbiAgICB2YXIgdGltZW91dCA9IG51bGw7XG4gICAgaWYgKHRoaXMub3B0aW9ucy50aW1lb3V0TXMpIHtcbiAgICAgIHRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgICAgZGVsZXRlIHRoaXMudHhuc1tzaWduYWwudHJhbnNhY3Rpb25dO1xuICAgICAgICByZWplY3QobmV3IEVycm9yKFwiU2lnbmFsbGluZyBtZXNzYWdlIHRpbWVkIG91dC5cIikpO1xuICAgICAgfSwgdGhpcy5vcHRpb25zLnRpbWVvdXRNcyk7XG4gICAgfVxuICAgIHRoaXMudHhuc1tzaWduYWwudHJhbnNhY3Rpb25dID0geyByZXNvbHZlOiByZXNvbHZlLCByZWplY3Q6IHJlamVjdCwgdGltZW91dDogdGltZW91dCB9O1xuICAgIHRoaXMub3V0cHV0KEpTT04uc3RyaW5naWZ5KHNpZ25hbCkpO1xuICAgIHRoaXMuX3Jlc2V0S2VlcGFsaXZlKCk7XG4gIH0pO1xufTtcblxuSmFudXNTZXNzaW9uLnByb3RvdHlwZS5fcmVzZXRLZWVwYWxpdmUgPSBmdW5jdGlvbigpIHtcbiAgaWYgKHRoaXMua2VlcGFsaXZlVGltZW91dCkge1xuICAgIGNsZWFyVGltZW91dCh0aGlzLmtlZXBhbGl2ZVRpbWVvdXQpO1xuICB9XG4gIGlmICh0aGlzLm9wdGlvbnMua2VlcGFsaXZlTXMpIHtcbiAgICB0aGlzLmtlZXBhbGl2ZVRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IHRoaXMuX2tlZXBhbGl2ZSgpLCB0aGlzLm9wdGlvbnMua2VlcGFsaXZlTXMpO1xuICB9XG59O1xuXG5KYW51c1Nlc3Npb24ucHJvdG90eXBlLl9rZWVwYWxpdmUgPSBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIHRoaXMuc2VuZCh7IGphbnVzOiBcImtlZXBhbGl2ZVwiIH0pO1xufTtcblxubW9kdWxlLmV4cG9ydHMgPSB7XG4gIEphbnVzUGx1Z2luSGFuZGxlLFxuICBKYW51c1Nlc3Npb24sXG4gIHZlcmJvc2Vcbn07XG5cblxuXG4vLy8vLy8vLy8vLy8vLy8vLy9cbi8vIFdFQlBBQ0sgRk9PVEVSXG4vLyAuL25vZGVfbW9kdWxlcy9taW5pamFudXMvbWluaWphbnVzLmpzXG4vLyBtb2R1bGUgaWQgPSAxXG4vLyBtb2R1bGUgY2h1bmtzID0gMCJdLCJzb3VyY2VSb290IjoiIn0=