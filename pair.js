const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const FileType = require('file-type');
const fetch = require('node-fetch');
const { MongoClient } = require('mongodb');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegInstaller = require('@ffmpeg-installer/ffmpeg');
ffmpeg.setFfmpegPath(ffmpegInstaller.path);

const {
  default: makeWASocket,
  useMultiFileAuthState,
  delay,
  getContentType,
  makeCacheableSignalKeyStore,
  Browsers,
  jidNormalizedUser,
  downloadContentFromMessage,
  DisconnectReason
} = require('baileyz');

// ---------------- CONFIG ----------------

const BOT_NAME_FANCY = 'lakshan md';
const LOADING_GIF_URL = 'https://whiteshadow-uploader.vercel.app/files/20n.mp4';

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
  AUTO_LIKE_EMOJI: ['💙', '🩷', '💜', '🤎', '🧡', '🩵', '💛', '🩶', '♥️', '💗', '❤️‍🔥'],
  PREFIX: '.',
  MAX_RETRIES: 3,
  GROUP_INVITE_LINK: 'https://chat.whatsapp.com/JBIQDUg6f7g5AvExseAzO4?mode=hqctcla',
  CHANNEL_LINK: 'https://whatsapp.com/channel/0029VbAWcvCLY6dAjn0FnW0L',
  RCD_IMAGE_PATH: 'https://whiteshadow-uploder.zone.id/files/13z.jpg',
  NEWSLETTER_JID: '1201234567890@newsletter',
  OTP_EXPIRY: 300000,
  OWNER_NUMBER: process.env.OWNER_NUMBER || '94789227570',
  BOT_NAME: 'lakshan md',
  BOT_VERSION: '1.0.0V',
  OWNER_NAME: 'lakshan',
  IMAGE_PATH: 'https://whiteshadow-uploder.zone.id/files/13z.jpg',
  BOT_FOOTER: '> *lakshan md *',
  BUTTON_IMAGES: { ALIVE: 'https://whiteshadow-uploder.zone.id/files/13z.jpg' }
};

// ---------------- MONGO SETUP ----------------

const MONGO_URI = config.MONGO_URI;
const MONGO_DB = process.env.MONGO_DB || 'FREE';

let mongoClient, mongoDB;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol;

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

  await sessionsCol.createIndex({ number: 1 }, { unique: true });
  await numbersCol.createIndex({ number: 1 }, { unique: true });
  await newsletterCol.createIndex({ jid: 1 }, { unique: true });
  await newsletterReactsCol.createIndex({ jid: 1 }, { unique: true });
  await configsCol.createIndex({ number: 1 }, { unique: true });
  console.log('✅ Mongo initialized');
}

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

async function saveNewsletterReaction(jid, messageId, emoji, sessionNumber) {
  try {
    await initMongo();
    if (!mongoDB) await initMongo();
    const col = mongoDB.collection('newsletter_reactions_log');
    await col.insertOne({ jid, messageId, emoji, sessionNumber, ts: new Date() });
  } catch (e) { }
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

async function addNewsletterReactConfig(jid, emojis = []) {
  try {
    await initMongo();
    await newsletterReactsCol.updateOne({ jid }, { $set: { jid, emojis, addedAt: new Date() } }, { upsert: true });
  } catch (e) { throw e; }
}

async function removeNewsletterReactConfig(jid) {
  try {
    await initMongo();
    await newsletterReactsCol.deleteOne({ jid });
  } catch (e) { throw e; }
}

async function listNewsletterReactsFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterReactsCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { return []; }
}

function formatMessage(title, content, footer) {
  return `${title}\n\n${content}\n\n> *${footer}*`;
}

function generateOTP() { return Math.floor(100000 + Math.random() * 900000).toString(); }

function getSriLankaTimestamp() { return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }

const activeSockets = new Map();
const socketCreationTime = new Map();
const otpStore = new Map();

function getMetaMention(botName = BOT_NAME_FANCY) {
  const randomId = Math.random().toString(36).substring(2, 15);
  return {
    key: { 
      remoteJid: "status@broadcast", 
      participant: "0@s.whatsapp.net", 
      fromMe: false, 
      id: `META_AI_${randomId}` 
    },
    message: { 
      contactMessage: { 
        displayName: botName, 
        vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` 
      } 
    }
  };
}

// ---------------- SEND CIRCLE VIDEO ----------------

async function sendCircleVideo(socket, jid, caption = "", footer = "", buttons = null, metaQuote = null) {
  try {
    await socket.sendMessage(jid, {
      video: { url: LOADING_GIF_URL },
      gifPlayback: true,
      gifAttribution: 1,
      caption: caption,
      footer: footer,
      buttons: buttons,
      headerType: 4
    }, { quoted: metaQuote || getMetaMention() });
  } catch (error) {
    console.error('Send circle video error:', error);
    await socket.sendMessage(jid, { text: caption }, { quoted: metaQuote || getMetaMention() });
  }
}

async function sendWithCircleVideo(socket, jid, content, metaQuote = null) {
  try {
    const hasButtons = content.buttons && content.buttons.length > 0;
    
    if (content.image) {
      let imagePayload = typeof content.image === 'object' && content.image.url ? content.image.url : content.image;
      await sendCircleVideo(socket, jid, content.caption, content.footer, hasButtons ? content.buttons : null, metaQuote);
      
      if (typeof imagePayload === 'string' && imagePayload.startsWith('http')) {
        await socket.sendMessage(jid, { image: { url: imagePayload } }, { quoted: metaQuote || getMetaMention() });
      } else if (Buffer.isBuffer(imagePayload)) {
        await socket.sendMessage(jid, { image: imagePayload }, { quoted: metaQuote || getMetaMention() });
      }
    } else if (content.text) {
      await sendCircleVideo(socket, jid, content.text, "", null, metaQuote);
    } else {
      await sendCircleVideo(socket, jid, content.caption || "", content.footer || "", content.buttons || null, metaQuote);
    }
  } catch (error) {
    console.error('Send with circle video error:', error);
    await socket.sendMessage(jid, content, { quoted: metaQuote || getMetaMention() });
  }
}

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
      await delay(2000 * (config.MAX_RETRIES - retries));
    }
  }
  return { status: 'failed', error: 'Max retries reached' };
}

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
  
  const caption = `*╭━━━━━━━━━━━━━❥❥❥*
*┃ 🤖 ${botName}*
*┃ 📞 Number:* ${number}
*┃ 🏠 Group:* ${groupStatus}
*┃ 📰 Channel:* ${channelStatus}
*┃ 🕒 Connected:* ${getSriLankaTimestamp()}
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ Bot Connected Successfully!`;

  for (const admin of admins) {
    try {
      const to = admin.includes('@') ? admin : `${admin}@s.whatsapp.net`;
      await sendCircleVideo(socket, to, caption, botName, null, getMetaMention(botName));
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
    
    const caption = `*╭━━━━━━━━━━━━━❥❥❥*
*┃ 👑 Owner Notice*
*┃ 🤖 ${botName}*
*┃ 📞 Number:* ${number}
*┃ 🏠 Group:* ${groupStatus}
*┃ 📰 Channel:* ${channelStatus}
*┃ 🔢 Active:* ${activeCount}
*┃ 🕒 Time:* ${getSriLankaTimestamp()}
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ New Bot Connected!`;
    
    await sendCircleVideo(socket, ownerJid, caption, botName, null, getMetaMention(botName));
  } catch (err) { }
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = `*🔐 OTP VERIFICATION*\n\nYour OTP: *${otp}*\nExpires in 5 minutes.\n\nNumber: ${number}`;
  try { await socket.sendMessage(userJid, { text: message }); } catch (error) { throw error; }
}

// ---------------- NEWSLETTER HANDLERS ----------------

async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key) return;
    const jid = message.key.remoteJid;

    try {
      const followedDocs = await listNewslettersFromMongo();
      const reactConfigs = await listNewsletterReactsFromMongo();
      const reactMap = new Map();
      for (const r of reactConfigs) reactMap.set(r.jid, r.emojis || []);

      const followedJids = followedDocs.map(d => d.jid);
      if (!followedJids.includes(jid) && !reactMap.has(jid)) return;

      let emojis = reactMap.get(jid) || null;
      if ((!emojis || emojis.length === 0) && followedDocs.find(d => d.jid === jid)) {
        emojis = (followedDocs.find(d => d.jid === jid).emojis || []);
      }
      if (!emojis || emojis.length === 0) emojis = config.AUTO_LIKE_EMOJI;

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
          await saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber || null);
          break;
        } catch (err) {
          await delay(1200);
        }
      }
    } catch (error) { }
  });
}

// ---------------- STATUS HANDLERS ----------------

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

async function handleMessageRevocation(socket, number) {
  socket.ev.on('messages.delete', async ({ keys }) => {
    if (!keys || keys.length === 0) return;
    const messageKey = keys[0];
    const userJid = jidNormalizedUser(socket.user.id);
    const deletionTime = getSriLankaTimestamp();
    const message = `*🗑️ MESSAGE DELETED*\n\nFrom: ${messageKey.remoteJid}\nTime: ${deletionTime}`;
    try { await socket.sendMessage(userJid, { text: message }); } catch (error) { }
  });
}

// ---------------- CALL REJECTION ----------------

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

// ---------------- AUTO MESSAGE READ ----------------

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

// ---------------- TYPING/RECORDING HANDLERS ----------------

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

// ---------------- GROUP HELPERS ----------------

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

// ---------------- COMMAND HANDLERS ----------------

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
      : msg.message?.extendedTextMessage?.contextInfo?.hasOwnProperty('quotedMessage')
        ? msg.message.extendedTextMessage.text
        : (type == 'interactiveResponseMessage')
          ? msg.message.interactiveResponseMessage?.nativeFlowResponseMessage
          && JSON.parse(msg.message.interactiveResponseMessage.nativeFlowResponseMessage.paramsJson)?.id
          : (type == 'templateButtonReplyMessage')
            ? msg.message.templateButtonReplyMessage?.selectedId
            : (type === 'extendedTextMessage')
              ? msg.message.extendedTextMessage.text
              : (type == 'imageMessage') && msg.message.imageMessage.caption
                ? msg.message.imageMessage.caption
                : (type == 'videoMessage') && msg.message.videoMessage.caption
                  ? msg.message.videoMessage.caption
                  : (type == 'buttonsResponseMessage')
                    ? msg.message.buttonsResponseMessage?.selectedButtonId
                    : (type == 'listResponseMessage')
                      ? msg.message.listResponseMessage?.singleSelectReply?.selectedRowId
                      : (type === 'viewOnceMessage')
                        ? msg.message[type]?.message[getContentType(msg.message[type].message)]
                        : '';
    body = String(body || '');

    if (!body || typeof body !== 'string') return;

    const prefix = config.PREFIX;
    const isCmd = body.startsWith(prefix);
    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);

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
      const metaQuote = getMetaMention(botName);

      // ==================== NUMBER REPLY SYSTEM ====================
      if (body.match(/^[0-9]+$/) && body.length >= 5 && body.length <= 15 && !isCmd) {
        const repliedNumber = body;
        
        if (repliedNumber === '1') {
          // OWNER MENU
          const ownerMenu = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}✍︎*
*╰────────────────*
*┏━━━━━━━━━━━━━*
*┃*     *👑 OWNER MENU*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *👤 OWNER* - ${config.OWNER_NAME}
*┃* *📞 NUMBER* - ${config.OWNER_NUMBER}
*┃* *🛠️ COMMANDS* - shutdown, restart, broadcast
*┃* *👥 ADMIN* - addadmin, removeadmin, listadmin
*┗━━━━━━━━━━━━━*
*┏━「 Reply Number ⤵️ 」*
*┃* *0️⃣ BACK TO MAIN MENU*
*┗━━━━━━━━━━━━━*`;
          await sendCircleVideo(socket, sender, ownerMenu, botName, null, metaQuote);
        }
        else if (repliedNumber === '2') {
          // SOCIAL MENU
          const socialMenu = `
*┊ ☠︎︎*
*✧  ${botName}𝄞*
*╰────────────────*
*┏━━━━━━━━━━━━━*
*┃*     *🌐 SOCIAL MENU*
*┗━━━━━━━━━━━━━*
*┏━━━━━━━━━━━━━*
*┃* *📱 WHATSAPP* - ${config.CHANNEL_LINK || 'Not set'}
*┃* *📺 CHANNEL* - ${config.CHANNEL_LINK || 'Not set'}
*┃* *👥 GROUP* - ${config.GROUP_INVITE_LINK || 'Not set'}
*┗━━━━━━━━━━━━━*
*┏━「 Reply Number ⤵️ 」*
*┃* *0️⃣ BACK TO MAIN MENU*
*┗━━━━━━━━━━━━━❥❥❥*`;
          await sendCircleVideo(socket, sender, socialMenu, botName, null, metaQuote);
        }
        else if (repliedNumber === '3') {
          // AI MENU
          const aiMenu = `
*┊ ☠︎︎*
*✧  ${botName}𝄞*
*╰────────────────*
*┏━━━━━━━━━━━━━*
*┃*     *🤖 AI MENU*
*┗━━━━━━━━━━━━━*
*┏━━━━━━━━━━━━━*
*┃* *💬 GPT* - .gpt <question>
*┃* *🎨 IMAGE* - .img <prompt>
*┃* *📝 SUMMARIZE* - .summarize
*┃* *🌍 TRANSLATE* - .tr <lang> <text>
*┗━━━━━━━━━━━━━*
*┏━「 Reply Number ⤵️ 」*
*┃* *0️⃣ BACK TO MAIN MENU*
*┗━━━━━━━━━━━━━*`;
          await sendCircleVideo(socket, sender, aiMenu, botName, null, metaQuote);
        }
        else if (repliedNumber === '4') {
          // GROUP MENU
          const groupMenu = `
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎*
*╰────────────────❂*
*┏━━━━━━━━━━━━━*
*┃*     *👥 GROUP MENU*
*┗━━━━━━━━━━━━━*
*┏━━━━━━━━━━━━━*
*┃* *🚪 KICK* - .kick @user
*┃* *➕ ADD* - .add 947xxxxxxxx
*┃* *⬆️ PROMOTE* - .promote @user
*┃* *⬇️ DEMOTE* - .demote @user
*┃* *📢 TAGALL* - .tagall
*┃* *🚶 LEAVE* - .leave
*┃* *📊 GROUPINFO* - .groupinfo
*┗━━━━━━━━━━━━━*
*┏━「 Reply Number ⤵️ 」*
*┃* *0️⃣ BACK TO MAIN MENU*
*┗━━━━━━━━━━━━━❥❥❥*`;
          await sendCircleVideo(socket, sender, groupMenu, botName, null, metaQuote);
        }
        else if (repliedNumber === '5') {
          // TOOLS MENU
          const toolsMenu = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *🛠️ TOOLS MENU*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *🖼️ GETDP* - .getdp
*┃* *ℹ️ ABOUT* - .about 947xxxxxxxx
*┃* *📰 FOLLOW* - .follow jid
*┃* *🔔 UNFOLLOW* - .unfollow jid
*┃* *❤️ REACT* - .chr jid/id,emoji
*┗━━━━━━━━━━━━━❥❥❥*
*┏━「 Reply Number ⤵️ 」*
*┃* *0️⃣ BACK TO MAIN MENU*
*┗━━━━━━━━━━━━━❥❥❥*`;
          await sendCircleVideo(socket, sender, toolsMenu, botName, null, metaQuote);
        }
        else if (repliedNumber === '6') {
          // EDUCATION MENU
          const eduMenu = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *📚 EDUCATION MENU*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *📖 WIKI* - .wiki <query>
*┃* *📰 NEWS* - .news
*┃* *📝 DEFINE* - .define <word>
*┃* *🔢 CALC* - .calc <expression>
*┗━━━━━━━━━━━━━❥❥❥*
*┏━「 Reply Number ⤵️ 」*
*┃* *0️⃣ BACK TO MAIN MENU*
*┗━━━━━━━━━━━━━❥❥❥*`;
          await sendCircleVideo(socket, sender, eduMenu, botName, null, metaQuote);
        }
        else if (repliedNumber === '7') {
          // CHANNEL MENU
          const channelMenu = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *📺 CHANNEL MENU*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *🔗 CHANNEL LINK* - ${config.CHANNEL_LINK || 'Not set'}
*┃* *👥 GROUP LINK* - ${config.GROUP_INVITE_LINK || 'Not set'}
*┃* *📰 FOLLOW CHANNEL* - .follow channel
*┃* *🔔 UNFOLLOW* - .unfollow
*┗━━━━━━━━━━━━━❥❥❥*
*┏━「 Reply Number ⤵️ 」*
*┃* *0️⃣ BACK TO MAIN MENU*
*┗━━━━━━━━━━━━━❥❥❥*`;
          await sendCircleVideo(socket, sender, channelMenu, botName, null, metaQuote);
        }
        else if (repliedNumber === '0') {
          // BACK TO MAIN MENU - Trigger menu command
          const mainMenu = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}☠️𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     * MAIN MENU*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *☠️ OWNER* - ${config.OWNER_NAME}
*┃* *👁️ VERSION* - ${config.BOT_VERSION}
*┃* *⚡ COMMANDS* - ${Object.keys(commandHandlers).length}+
*┃* *🔧 PREFIX* - [ ${prefix} ]
*┃* *🟢 ACTIVE BOTS* - ${activeSockets.size}
*┃* *🌐 WEB* - ${process.env.WEB_URL || 'Not set'}
*┃* *🎬 MEDIA* - Video/Circle Support
*┗━━━━━━━━━━━━━❥❥❥*
*┏━「 Reply Number ⤵️ 」*
*┃* *1️⃣ OWNER MENU*
*┃* *2️⃣ SOCIAL MENU*
*┃* *3️⃣ AI MENU*
*┃* *4️⃣ GROUP MENU*
*┃* *5️⃣ TOOLS MENU*
*┃* *6️⃣ EDUCATION MENU*
*┃* *7️⃣ CHANNEL MENU*
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ ${botName} | Type ${prefix}help for commands`;
          await sendCircleVideo(socket, sender, mainMenu, botName, null, metaQuote);
        }
        return;
      }

      // ==================== SETTINGS PANEL WITH NUMBER REPLY ====================
      if (command === 'setting') {
        const subCommand = args[0]?.toLowerCase();
        
        if (!subCommand) {
          // Show settings panel with numbers
          const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
          
          const settingsPanel = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *⚙️ SETTINGS PANEL*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *1️⃣ AUTO VIEW STATUS* - ${currentConfig.AUTO_VIEW_STATUS || config.AUTO_VIEW_STATUS}
*┃* *2️⃣ AUTO LIKE STATUS* - ${currentConfig.AUTO_LIKE_STATUS || config.AUTO_LIKE_STATUS}
*┃* *3️⃣ AUTO RECORDING* - ${currentConfig.AUTO_RECORDING || config.AUTO_RECORDING}
*┃* *4️⃣ AUTO TYPING* - ${currentConfig.AUTO_TYPING || config.AUTO_TYPING}
*┃* *5️⃣ AUTO READ MSG* - ${currentConfig.AUTO_READ_MESSAGE || config.AUTO_READ_MESSAGE}
*┃* *6️⃣ ANTI CALL* - ${currentConfig.ANTI_CALL || config.ANTI_CALL}
*┃* *7️⃣ WORK TYPE* - ${currentConfig.WORK_TYPE || config.WORK_TYPE}
*┃* *8️⃣ BOT NAME* - ${currentConfig.botName || botName}
*┃* *9️⃣ PREFIX* - ${currentConfig.PREFIX || prefix}
*┗━━━━━━━━━━━━━❥❥❥*
*┏━「 Reply Number to Toggle ⤵️ 」*
*┃* *1* - Toggle Auto View Status
*┃* *2* - Toggle Auto Like Status
*┃* *3* - Toggle Auto Recording
*┃* *4* - Toggle Auto Typing
*┃* *5* - Change Auto Read (all/cmd/off)
*┃* *6* - Toggle Anti Call
*┃* *7* - Change Work Type
*┃* *0* - RESET ALL SETTINGS
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ Reply with number to change setting`;
          await sendCircleVideo(socket, sender, settingsPanel, botName, null, metaQuote);
          return;
        }
        
        // Handle number replies for settings
        if (subCommand.match(/^[0-9]+$/)) {
          const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
          const option = subCommand;
          
          if (option === '1') {
            const newValue = currentConfig.AUTO_VIEW_STATUS === 'true' ? 'false' : 'true';
            currentConfig.AUTO_VIEW_STATUS = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto View Status turned *${newValue === 'true' ? 'ON' : 'OFF'}*`, botName, null, metaQuote);
          }
          else if (option === '2') {
            const newValue = currentConfig.AUTO_LIKE_STATUS === 'true' ? 'false' : 'true';
            currentConfig.AUTO_LIKE_STATUS = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto Like Status turned *${newValue === 'true' ? 'ON' : 'OFF'}*`, botName, null, metaQuote);
          }
          else if (option === '3') {
            const newValue = currentConfig.AUTO_RECORDING === 'true' ? 'false' : 'true';
            currentConfig.AUTO_RECORDING = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto Recording turned *${newValue === 'true' ? 'ON' : 'OFF'}*`, botName, null, metaQuote);
          }
          else if (option === '4') {
            const newValue = currentConfig.AUTO_TYPING === 'true' ? 'false' : 'true';
            currentConfig.AUTO_TYPING = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto Typing turned *${newValue === 'true' ? 'ON' : 'OFF'}*`, botName, null, metaQuote);
          }
          else if (option === '5') {
            const modes = ['all', 'cmd', 'off'];
            const currentIndex = modes.indexOf(currentConfig.AUTO_READ_MESSAGE || config.AUTO_READ_MESSAGE);
            const newIndex = (currentIndex + 1) % modes.length;
            currentConfig.AUTO_READ_MESSAGE = modes[newIndex];
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto Read Message set to *${modes[newIndex]}*`, botName, null, metaQuote);
          }
          else if (option === '6') {
            const newValue = currentConfig.ANTI_CALL === 'on' ? 'off' : 'on';
            currentConfig.ANTI_CALL = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Anti Call turned *${newValue === 'on' ? 'ON' : 'OFF'}*`, botName, null, metaQuote);
          }
          else if (option === '7') {
            const types = ['public', 'private', 'inbox', 'groups'];
            const currentIndex = types.indexOf(currentConfig.WORK_TYPE || config.WORK_TYPE);
            const newIndex = (currentIndex + 1) % types.length;
            currentConfig.WORK_TYPE = types[newIndex];
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Work Type set to *${types[newIndex]}*`, botName, null, metaQuote);
          }
          else if (option === '8') {
            await sendCircleVideo(socket, sender, `📝 Send new bot name as reply:`, botName, null, metaQuote);
            // Wait for reply
            const replyHandler = async (msg) => {
              const replyBody = msg.message?.conversation || msg.message?.extendedTextMessage?.text;
              if (replyBody && msg.key.remoteJid === sender && !replyBody.startsWith(prefix)) {
                currentConfig.botName = replyBody;
                await setUserConfigInMongo(sanitized, currentConfig);
                await sendCircleVideo(socket, sender, `✅ Bot name changed to *${replyBody}*`, botName, null, getMetaMention(replyBody));
                socket.ev.off('messages.upsert', replyHandler);
              }
            };
            socket.ev.on('messages.upsert', replyHandler);
            setTimeout(() => socket.ev.off('messages.upsert', replyHandler), 30000);
          }
          else if (option === '9') {
            await sendCircleVideo(socket, sender, `📝 Send new prefix as reply:`, botName, null, metaQuote);
            const replyHandler = async (msg) => {
              const replyBody = msg.message?.conversation || msg.message?.extendedTextMessage?.text;
              if (replyBody && msg.key.remoteJid === sender && !replyBody.startsWith(prefix) && replyBody.length === 1) {
                currentConfig.PREFIX = replyBody;
                await setUserConfigInMongo(sanitized, currentConfig);
                await sendCircleVideo(socket, sender, `✅ Prefix changed to *${replyBody}*`, botName, null, getMetaMention(botName));
                socket.ev.off('messages.upsert', replyHandler);
              }
            };
            socket.ev.on('messages.upsert', replyHandler);
            setTimeout(() => socket.ev.off('messages.upsert', replyHandler), 30000);
          }
          else if (option === '0') {
            await setUserConfigInMongo(sanitized, {});
            await sendCircleVideo(socket, sender, `✅ *ALL SETTINGS RESET TO DEFAULT*`, botName, null, metaQuote);
          }
          return;
        }
        
        // Handle setting set command
        if (subCommand === 'set') {
          const key = args[1];
          const value = args.slice(2).join(' ');
          if (!key || !value) {
            await sendCircleVideo(socket, sender, `❌ Usage: ${prefix}setting set <key> <value>\n\nKeys: botName, prefix, autoViewStatus, autoLikeStatus, autoRecording, autoTyping, autoReadMessage, antiCall, workType`, botName, null, metaQuote);
            return;
          }
          const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
          let updatedValue = value;
          if (key === 'autoViewStatus' || key === 'autoLikeStatus' || key === 'autoRecording' || key === 'autoTyping') {
            updatedValue = value === 'true' ? 'true' : 'false';
          } else if (key === 'autoReadMessage') {
            if (!['all', 'cmd', 'off'].includes(value)) {
              await sendCircleVideo(socket, sender, `❌ Use: all, cmd, or off`, botName, null, metaQuote);
              return;
            }
          } else if (key === 'antiCall') {
            if (!['on', 'off'].includes(value)) {
              await sendCircleVideo(socket, sender, `❌ Use: on or off`, botName, null, metaQuote);
              return;
            }
          } else if (key === 'workType') {
            if (!['public', 'private', 'inbox', 'groups'].includes(value)) {
              await sendCircleVideo(socket, sender, `❌ Use: public, private, inbox, or groups`, botName, null, metaQuote);
              return;
            }
          }
          currentConfig[key] = updatedValue;
          await setUserConfigInMongo(sanitized, currentConfig);
          await sendCircleVideo(socket, sender, `✅ *${key}* = \`${updatedValue}\``, botName, null, metaQuote);
          return;
        }
        
        await sendCircleVideo(socket, sender, `❌ Unknown setting command. Use ${prefix}setting or ${prefix}setting set`, botName, null, metaQuote);
        return;
      }

      // ==================== OTHER COMMANDS WITH CIRCLE VIDEO ====================
      switch (command) {
        case 'alive':
        case 'alive2': {
          const now = new Date();
          const sriLankaDate = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Colombo' }));
          const currentHour = sriLankaDate.getHours();
          let greeting = currentHour >= 5 && currentHour < 12 ? 'Good Morning 🌅' : currentHour >= 12 && currentHour < 18 ? 'Good Afternoon' : 'Good Evening 🌙';
          
          const startTime = socketCreationTime.get(number) || Date.now();
          const uptime = Math.floor((Date.now() - startTime) / 1000);
          const hours = Math.floor(uptime / 3600);
          const minutes = Math.floor((uptime % 3600) / 60);
          const seconds = Math.floor(uptime % 60);
          
          const aliveText = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *🤖 BOT ALIVE*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *🗯️ GREETING* - ${greeting}
*┃* *🗓️ DATE* - ${sriLankaDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric' })}
*┃* *📆 DAY* - ${sriLankaDate.toLocaleDateString('en-US', { weekday: 'long' })}
*┃* *⏱️ TIME* - ${sriLankaDate.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true })}
*┃* *📄 BOT NAME* - ${botName}
*┃* *🥷 OWNER* - ${config.OWNER_NAME}
*┃* *🧬 VERSION* - ${config.BOT_VERSION}
*┃* *📟 UPTIME* - ${hours}h ${minutes}m ${seconds}s
*┃* *✒️ PREFIX* - ${prefix}
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ ${botName} is Active!`;
          
          await sendCircleVideo(socket, sender, aliveText, botName, null, metaQuote);
          break;
        }

        case 'ping': {
          const start = Date.now();
          const end = Date.now();
          const latency = end - start;
          const speedStatus = latency < 200 ? 'Excellent 🟢' : latency < 500 ? 'Good 🟡' : 'Slow 🔴';
          
          const pingText = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *🏓 PONG!*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *⚡ SPEED* - ${latency}ms
*┃* *📡 STATUS* - ${speedStatus}
*┃* *🤖 BOT* - ${botName}
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ Response Time: ${latency}ms`;
          
          await sendCircleVideo(socket, sender, pingText, botName, null, metaQuote);
          break;
        }

        case 'menu':
        case 'help': {
          const mainMenu = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *🏠 MAIN MENU*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *👑 OWNER* - ${config.OWNER_NAME}
*┃* *📌 VERSION* - ${config.BOT_VERSION}
*┃* *⚡ COMMANDS* - 25+
*┃* *🔧 PREFIX* - [ ${prefix} ]
*┃* *🟢 ACTIVE BOTS* - ${activeSockets.size}
*┃* *🎬 MEDIA* - Circle Video Support
*┗━━━━━━━━━━━━━❥❥❥*
*┏━「 Reply Number ⤵️ 」*
*┃* *1️⃣ OWNER MENU*
*┃* *2️⃣ SOCIAL MENU*
*┃* *3️⃣ AI MENU*
*┃* *4️⃣ GROUP MENU*
*┃* *5️⃣ TOOLS MENU*
*┃* *6️⃣ EDUCATION MENU*
*┃* *7️⃣ CHANNEL MENU*
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ Type ${prefix}setting for settings panel`;
          
          await sendCircleVideo(socket, sender, mainMenu, botName, null, metaQuote);
          break;
        }

        case 'system': {
          const os = require('os');
          const sysText = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *🖥️ SYSTEM INFO*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *🧸 OS* - ${os.type()} ${os.release()}
*┃* *📡 PLATFORM* - ${os.platform()}
*┃* *🧠 CPU CORES* - ${os.cpus().length}
*┃* *💾 MEMORY* - ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(2)} GB
*┃* *🤖 BOT* - ${botName}
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ System Information`;
          
          await sendCircleVideo(socket, sender, sysText, botName, null, metaQuote);
          break;
        }

        case 'getdp': {
          let targetJid = sender;
          if (args[0]) {
            const argNumber = args[0].replace(/[^0-9]/g, '');
            if (argNumber) targetJid = `${argNumber}@s.whatsapp.net`;
          }
          const ppUrl = await getUserProfilePicture(socket, targetJid);
          if (ppUrl) {
            await socket.sendMessage(sender, { image: { url: ppUrl }, caption: `📸 Profile Picture of ${targetJid.split('@')[0]}` }, { quoted: msg });
          } else {
            await sendCircleVideo(socket, sender, `❌ No profile picture found for ${targetJid.split('@')[0]}`, botName, null, metaQuote);
          }
          break;
        }

        // ==================== REACTION COMMAND (chr) ====================
        case 'chr': {
          const q = args.join(' ').trim();
          if (!q.includes(',')) {
            await sendCircleVideo(socket, sender, `❌ Usage: ${prefix}chr <channelJid/messageId>,<emoji>\n\nExample: ${prefix}chr 120363334838967293@newsletter/BAEB4p4CJIq,❤️`, botName, null, metaQuote);
            break;
          }

          const parts = q.split(',');
          let channelRef = parts[0].trim();
          const reactEmoji = parts[1].trim();

          let channelJid = channelRef;
          let messageId = null;
          const maybeParts = channelRef.split('/');
          if (maybeParts.length >= 2) {
            messageId = maybeParts[maybeParts.length - 1];
            channelJid = maybeParts[maybeParts.length - 2].includes('@newsletter') ? maybeParts[maybeParts.length - 2] : channelJid;
          }

          if (!channelJid.endsWith('@newsletter')) {
            if (/^\d+$/.test(channelJid)) channelJid = `${channelJid}@newsletter`;
          }

          if (!channelJid.endsWith('@newsletter') || !messageId) {
            await sendCircleVideo(socket, sender, `❌ Provide channelJid/messageId format.\nExample: ${prefix}chr 120363334838967293@newsletter/BAEB4p4CJIq,❤️`, botName, null, metaQuote);
            break;
          }

          try {
            await socket.newsletterReactMessage(channelJid, messageId.toString(), reactEmoji);
            await saveNewsletterReaction(channelJid, messageId.toString(), reactEmoji, sanitized);
            
            const reactText = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *✅ REACTION SUCCESS*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *📺 CHANNEL* - ${channelJid}
*┃* *🆔 MESSAGE* - ${messageId}
*┃* *❤️ EMOJI* - ${reactEmoji}
*┃* *👤 BY* - @${senderNumber}
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ Reaction added successfully!`;
            
            await sendCircleVideo(socket, sender, reactText, botName, null, metaQuote);
          } catch (e) {
            await sendCircleVideo(socket, sender, `❌ Failed to react: ${e.message || e}`, botName, null, metaQuote);
          }
          break;
        }

        // ==================== GROUP COMMANDS ====================
        case 'kick': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName, null, metaQuote); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName, null, metaQuote); break; }
          if (!(await isBotAdmin(socket, from))) { await sendCircleVideo(socket, sender, `❌ Bot not admin`, botName, null, metaQuote); break; }
          
          const mentioned = msg.message.extendedTextMessage?.contextInfo?.mentionedJid || [];
          if (mentioned.length === 0) { await sendCircleVideo(socket, sender, `❌ Tag user to kick`, botName, null, metaQuote); break; }
          
          for (const user of mentioned) {
            if (user === `${botNumber}@s.whatsapp.net`) continue;
            await socket.groupParticipantsUpdate(from, [user], 'remove');
          }
          await sendCircleVideo(socket, sender, `✅ Kicked ${mentioned.length} user(s)`, botName, null, metaQuote);
          break;
        }

        case 'add': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName, null, metaQuote); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName, null, metaQuote); break; }
          if (!(await isBotAdmin(socket, from))) { await sendCircleVideo(socket, sender, `❌ Bot not admin`, botName, null, metaQuote); break; }
          
          const numberToAdd = args[0]?.replace(/[^0-9]/g, '');
          if (!numberToAdd) { await sendCircleVideo(socket, sender, `❌ Provide number: ${prefix}add 947xxxxxxxx`, botName, null, metaQuote); break; }
          
          await socket.groupParticipantsUpdate(from, [`${numberToAdd}@s.whatsapp.net`], 'add');
          await sendCircleVideo(socket, sender, `✅ Added ${numberToAdd}`, botName, null, metaQuote);
          break;
        }

        case 'promote': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName, null, metaQuote); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName, null, metaQuote); break; }
          if (!(await isBotAdmin(socket, from))) { await sendCircleVideo(socket, sender, `❌ Bot not admin`, botName, null, metaQuote); break; }
          
          const mentioned = msg.message.extendedTextMessage?.contextInfo?.mentionedJid || [];
          if (mentioned.length === 0) { await sendCircleVideo(socket, sender, `❌ Tag user to promote`, botName, null, metaQuote); break; }
          
          for (const user of mentioned) await socket.groupParticipantsUpdate(from, [user], 'promote');
          await sendCircleVideo(socket, sender, `✅ Promoted ${mentioned.length} user(s)`, botName, null, metaQuote);
          break;
        }

        case 'demote': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName, null, metaQuote); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName, null, metaQuote); break; }
          if (!(await isBotAdmin(socket, from))) { await sendCircleVideo(socket, sender, `❌ Bot not admin`, botName, null, metaQuote); break; }
          
          const mentioned = msg.message.extendedTextMessage?.contextInfo?.mentionedJid || [];
          if (mentioned.length === 0) { await sendCircleVideo(socket, sender, `❌ Tag user to demote`, botName, null, metaQuote); break; }
          
          for (const user of mentioned) await socket.groupParticipantsUpdate(from, [user], 'demote');
          await sendCircleVideo(socket, sender, `✅ Demoted ${mentioned.length} user(s)`, botName, null, metaQuote);
          break;
        }

        case 'tagall': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName, null, metaQuote); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName, null, metaQuote); break; }
          
          const groupMetadata = await socket.groupMetadata(from);
          let mentions = [];
          let tagText = `📢 *TAG ALL* - ${groupMetadata.participants.length} members\n\n`;
          for (const participant of groupMetadata.participants) {
            mentions.push(participant.id);
            tagText += `• @${participant.id.split('@')[0]}\n`;
          }
          await socket.sendMessage(from, { text: tagText, mentions }, { quoted: msg });
          break;
        }

        case 'leave': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName, null, metaQuote); break; }
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName, null, metaQuote); break; }
          
          await sendCircleVideo(socket, sender, `👋 Goodbye!`, botName, null, metaQuote);
          await delay(2000);
          await socket.groupLeave(from);
          break;
        }

        case 'groupinfo': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName, null, metaQuote); break; }
          
          const groupMetadata = await socket.groupMetadata(from);
          const groupAdmins = groupMetadata.participants.filter(p => p.admin === 'admin' || p.admin === 'superadmin');
          const groupOwner = groupMetadata.owner || 'Unknown';
          
          const infoText = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${botName}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *📊 GROUP INFO*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *📛 NAME* - ${groupMetadata.subject}
*┃* *👥 MEMBERS* - ${groupMetadata.participants.length}
*┃* *👑 OWNER* - @${groupOwner.split('@')[0]}
*┃* *👮 ADMINS* - ${groupAdmins.length}
*┃* *📅 CREATED* - ${new Date(groupMetadata.creation * 1000).toLocaleDateString()}
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ Group Information`;
          
          await sendCircleVideo(socket, sender, infoText, botName, null, metaQuote);
          break;
        }

        // ==================== ADMIN COMMANDS ====================
        case 'addadmin': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName, null, metaQuote); break; }
          const adminJid = args[0];
          if (!adminJid) { await sendCircleVideo(socket, sender, `❌ Provide JID: ${prefix}addadmin 947xxxxxxxx`, botName, null, metaQuote); break; }
          const formattedJid = adminJid.includes('@') ? adminJid : `${adminJid.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
          await addAdminToMongo(formattedJid);
          await sendCircleVideo(socket, sender, `✅ Added ${formattedJid} as admin`, botName, null, metaQuote);
          break;
        }

        case 'removeadmin': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName, null, metaQuote); break; }
          const adminJid = args[0];
          if (!adminJid) { await sendCircleVideo(socket, sender, `❌ Provide JID`, botName, null, metaQuote); break; }
          const formattedJid = adminJid.includes('@') ? adminJid : `${adminJid.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
          await removeAdminFromMongo(formattedJid);
          await sendCircleVideo(socket, sender, `✅ Removed ${formattedJid} from admins`, botName, null, metaQuote);
          break;
        }

        case 'listadmin': {
          const admins = await loadAdminsFromMongo();
          if (admins.length === 0) {
            await sendCircleVideo(socket, sender, `📋 No admins found`, botName, null, metaQuote);
          } else {
            let adminList = `📋 *ADMIN LIST*\n\n`;
            admins.forEach((admin, i) => { adminList += `${i + 1}. ${admin}\n`; });
            await sendCircleVideo(socket, sender, adminList, botName, null, metaQuote);
          }
          break;
        }

        // ==================== NEWSLETTER COMMANDS ====================
        case 'follow': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName, null, metaQuote); break; }
          const newsletterJid = args[0];
          if (!newsletterJid) { await sendCircleVideo(socket, sender, `❌ Provide JID: ${prefix}follow 120363334838967293@newsletter`, botName, null, metaQuote); break; }
          await addNewsletterToMongo(newsletterJid, args.slice(1));
          if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(newsletterJid);
          await sendCircleVideo(socket, sender, `✅ Following: ${newsletterJid}`, botName, null, metaQuote);
          break;
        }

        case 'unfollow': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName, null, metaQuote); break; }
          const newsletterJid = args[0];
          if (!newsletterJid) { await sendCircleVideo(socket, sender, `❌ Provide JID`, botName, null, metaQuote); break; }
          await removeNewsletterFromMongo(newsletterJid);
          if (typeof socket.newsletterUnfollow === 'function') await socket.newsletterUnfollow(newsletterJid);
          await sendCircleVideo(socket, sender, `✅ Unfollowed: ${newsletterJid}`, botName, null, metaQuote);
          break;
        }

        // ==================== OWNER COMMANDS ====================
        case 'shutdown': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName, null, metaQuote); break; }
          await sendCircleVideo(socket, sender, `🔄 Shutting down...`, botName, null, metaQuote);
          process.exit(0);
          break;
        }

        case 'restart': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName, null, metaQuote); break; }
          await sendCircleVideo(socket, sender, `🔄 Restarting...`, botName, null, metaQuote);
          exec(`pm2 restart ${process.env.PM2_NAME || 'CHATUWA-MINI-main'}`);
          break;
        }

        case 'broadcast': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName, null, metaQuote); break; }
          const broadcastMsg = args.join(' ');
          if (!broadcastMsg) { await sendCircleVideo(socket, sender, `❌ Provide message to broadcast`, botName, null, metaQuote); break; }
          
          const allNumbers = await getAllNumbersFromMongo();
          let successCount = 0;
          for (const num of allNumbers) {
            const sock = activeSockets.get(num);
            if (sock) {
              try {
                const userJid = jidNormalizedUser(sock.user.id);
                await sendCircleVideo(sock, userJid, `📢 *BROADCAST*\n\n${broadcastMsg}`, botName, null, getMetaMention(botName));
                successCount++;
              } catch (e) {}
            }
          }
          await sendCircleVideo(socket, sender, `✅ Broadcast sent to ${successCount} sessions`, botName, null, metaQuote);
          break;
        }

        default:
          // Unknown command - ignore
          break;
      }
    } catch (err) {
      console.error('Command error:', err);
      try {
        await sendCircleVideo(socket, sender, `❌ Error: ${err.message}`, BOT_NAME_FANCY, null, getMetaMention(BOT_NAME_FANCY));
      } catch (e) {}
    }
  });
}

// ---------------- CLEANUP ----------------

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

// ---------------- AUTO RESTART ----------------

function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode || lastDisconnect?.error?.statusCode;
      const isLoggedOut = statusCode === 401 || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('logged out'));
      if (isLoggedOut) {
        console.log(`User ${number} logged out. Cleaning up...`);
        try { await deleteSessionAndCleanup(number, socket); } catch (e) { }
      } else {
        console.log(`Connection closed for ${number}. Reconnecting...`);
        try {
          await delay(10000);
          activeSockets.delete(number.replace(/[^0-9]/g, ''));
          socketCreationTime.delete(number.replace(/[^0-9]/g, ''));
          const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
          await EmpirePair(number, mockRes);
        } catch (e) { }
      }
    }
  });
}

// ---------------- EMPIREPAIR ----------------

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
      if (!res.headersSent) res.send({ code });
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
          
          // Join group and follow channel
          const groupResult = await joinGroup(socket).catch(() => ({ status: 'failed', error: 'Not configured' }));
          const channelResult = await followChannel(socket).catch(() => ({ status: 'failed', error: 'Not supported' }));

          try {
            const newsletterListDocs = await listNewslettersFromMongo();
            for (const doc of newsletterListDocs) {
              try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(doc.jid); } catch (e) { }
            }
          } catch (e) { }

          activeSockets.set(sanitizedNumber, socket);
          await addNumberToMongo(sanitizedNumber);
          
          await sendAdminConnectMessage(socket, sanitizedNumber, groupResult, channelResult, {});
          await sendOwnerConnectMessage(socket, sanitizedNumber, groupResult, channelResult, {});
          
          // Send welcome message to bot itself
          const welcomeText = `
*┊ ┊ ✫ ˚㋛ ⋆｡ ❀*
*┊ ☠︎︎*
*✧  ${BOT_NAME_FANCY}𓂃✍︎𝄞*
*╰────────────────❂*
*┏━━━━━━━━━━━━━❥❥❥*
*┃*     *✅ BOT CONNECTED*
*┗━━━━━━━━━━━━━❥❥❥*
*┏━━━━━━━━━━━━━❥❥❥*
*┃* *📞 NUMBER* - ${sanitizedNumber}
*┃* *🏠 GROUP* - ${groupResult.status === 'success' ? '✅ Joined' : '❌ Failed'}
*┃* *📰 CHANNEL* - ${channelResult.status === 'success' ? '✅ Followed' : '❌ Failed'}
*┃* *🕒 TIME* - ${getSriLankaTimestamp()}
*┗━━━━━━━━━━━━━❥❥❥*

> ☠️ Bot is now active! Type .menu to start`;
          
          await sendCircleVideo(socket, userJid, welcomeText, BOT_NAME_FANCY, null, getMetaMention(BOT_NAME_FANCY));
        } catch (e) { }
      }
      if (connection === 'close') {
        try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch (e) { }
      }
    });

    activeSockets.set(sanitizedNumber, socket);

  } catch (error) {
    console.error('Pairing error:', error);
    socketCreationTime.delete(sanitizedNumber);
    if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
  }
}

// ---------------- EXPRESS ROUTES ----------------

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

// Dashboard
const dashboardStaticDir = path.join(__dirname, 'dashboard_static');
if (!fs.existsSync(dashboardStaticDir)) fs.ensureDirSync(dashboardStaticDir);
router.use('/dashboard/static', express.static(dashboardStaticDir));
router.get('/dashboard', async (req, res) => {
  res.sendFile(path.join(dashboardStaticDir, 'index.html'));
});

router.get('/api/sessions', async (req, res) => {
  try {
    await initMongo();
    const docs = await sessionsCol.find({}, { projection: { number: 1, updatedAt: 1 } }).sort({ updatedAt: -1 }).toArray();
    res.json({ ok: true, sessions: docs });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

router.get('/api/active', async (req, res) => {
  try {
    const keys = Array.from(activeSockets.keys());
    res.json({ ok: true, active: keys, count: keys.length });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

router.post('/api/session/delete', async (req, res) => {
  try {
    const { number } = req.body;
    if (!number) return res.status(400).json({ ok: false, error: 'number required' });
    const sanitized = ('' + number).replace(/[^0-9]/g, '');
    const running = activeSockets.get(sanitized);
    if (running) {
      try { if (typeof running.logout === 'function') await running.logout().catch(() => {}); } catch (e) {}
      try { running.ws?.close(); } catch (e) {}
      activeSockets.delete(sanitized);
      socketCreationTime.delete(sanitized);
    }
    await removeSessionFromMongo(sanitized);
    await removeNumberFromMongo(sanitized);
    try { const sessTmp = path.join(os.tmpdir(), `session_${sanitized}`); if (fs.existsSync(sessTmp)) fs.removeSync(sessTmp); } catch (e) {}
    res.json({ ok: true, message: `Session ${sanitized} removed` });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

router.get('/api/newsletters', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.json({ ok: true, list });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

router.get('/api/admins', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.json({ ok: true, list });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
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