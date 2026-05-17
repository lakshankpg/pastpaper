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
  NEWSLETTER_JID: '120363420783909652@newsletter',
  OTP_EXPIRY: 300000,
  OWNER_NUMBER: process.env.OWNER_NUMBER || '94789227570',
  BOT_NAME: 'laksha md',
  BOT_VERSION: '1.0.0V',
  OWNER_NAME: 'lakshan',
  IMAGE_PATH: 'https://whiteshadow-uploder.zone.id/files/13z.jpg',
  BOT_FOOTER: '> *lakshan md*',
  CIRCLE_VIDEO_URL: 'https://whiteshadow-uploader.vercel.app/files/20n.mp4'
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

function formatMessage(title, content, footer) {
  return `${title}\n\n${content}\n\n> *${footer}*`;
}

function generateOTP() { return Math.floor(100000 + Math.random() * 900000).toString(); }

function getSriLankaTimestamp() { return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }

const activeSockets = new Map();
const socketCreationTime = new Map();
const otpStore = new Map();
const numberReplyTracker = new Map();

// ---------------- SEND CIRCLE VIDEO FUNCTION ----------------

async function sendCircleVideo(socket, jid, caption = "", footer = "", mentionedJid = [], metaQuote = null) {
  try {
    await socket.sendMessage(jid, {
      video: { url: config.CIRCLE_VIDEO_URL },
      gifPlayback: true,
      gifAttribution: 1,
      caption: caption,
      footer: footer,
      headerType: 4
    }, { quoted: metaQuote, mentions: mentionedJid });
  } catch (error) {
    console.error('Send circle video error:', error);
    await socket.sendMessage(jid, { text: caption }, { quoted: metaQuote, mentions: mentionedJid });
  }
}

async function sendImageWithCircleVideo(socket, jid, imageUrl, caption, footer, mentionedJid = [], metaQuote = null) {
  try {
    await sendCircleVideo(socket, jid, caption, footer, mentionedJid, metaQuote);
    if (imageUrl) {
      await socket.sendMessage(jid, { image: { url: imageUrl } }, { quoted: metaQuote });
    }
  } catch (error) {
    console.error('Send image with circle error:', error);
    if (imageUrl) {
      await socket.sendMessage(jid, { image: { url: imageUrl }, caption: caption }, { quoted: metaQuote, mentions: mentionedJid });
    } else {
      await socket.sendMessage(jid, { text: caption }, { quoted: metaQuote, mentions: mentionedJid });
    }
  }
}

// ---------------- JOIN GROUP ----------------

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

// ---------------- FOLLOW CHANNEL ----------------

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
      await sendCircleVideo(socket, to, caption, botName);
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
    
    await sendCircleVideo(socket, ownerJid, caption, botName);
  } catch (err) { }
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = `🔐 *OTP VERIFICATION*\n\nYour OTP: *${otp}*\nExpires in 5 minutes.\n\nNumber: ${number}`;
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

// ---------------- DELETE MESSAGE NOTIFICATION ----------------

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
      try { await sendCircleVideo(socket, userJid, message, BOT_NAME_FANCY); } catch (error) { }
    } catch (error) { }
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

    // ==================== NUMBER REPLY SYSTEM ====================
    // Check if message is a number reply (not a command)
    if (body.match(/^[0-9]+$/) && body.length >= 1 && body.length <= 2 && !isCmd) {
      const repliedNumber = body;
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      const botName = userConfig.botName || BOT_NAME_FANCY;
      const logo = userConfig.logo || config.RCD_IMAGE_PATH;
      
      // Check if this number reply is from the same user who saw the menu
      const lastMenuTime = numberReplyTracker.get(senderNumber) || 0;
      if (Date.now() - lastMenuTime > 300000) {
        await sendCircleVideo(socket, sender, `❌ Menu expired. Please type ${prefix}menu again.`, botName);
        return;
      }
      
      if (repliedNumber === '1') {
        const ownerMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *👑 OWNER MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *👤 Owner:* ${config.OWNER_NAME}
┃ *📞 Number:* ${config.OWNER_NUMBER}
┃ *🛠️ Commands:* shutdown, restart, broadcast
┃ *👥 Admin:* addadmin, removeadmin, listadmin
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN MENU
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Owner Menu`;
        await sendImageWithCircleVideo(socket, sender, logo, ownerMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '2') {
        const socialMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *🌐 SOCIAL MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📱 WhatsApp:* ${config.CHANNEL_LINK || 'Not set'}
┃ *📺 Channel:* ${config.CHANNEL_LINK || 'Not set'}
┃ *👥 Group:* ${config.GROUP_INVITE_LINK || 'Not set'}
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN MENU
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Social Menu`;
        await sendImageWithCircleVideo(socket, sender, logo, socialMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '3') {
        const aiMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *🤖 AI MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *💬 Coming Soon...*
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN MENU
╰━━━━━━━━━━━━━❥❥❥

> ☠️ AI Menu`;
        await sendImageWithCircleVideo(socket, sender, logo, aiMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '4') {
        const groupMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *👥 GROUP MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🚪 Kick:* ${prefix}kick @user
┃ *➕ Add:* ${prefix}add 947xxxxxxxx
┃ *⬆️ Promote:* ${prefix}promote @user
┃ *⬇️ Demote:* ${prefix}demote @user
┃ *📢 Tagall:* ${prefix}tagall
┃ *🚶 Leave:* ${prefix}leave
┃ *📊 Groupinfo:* ${prefix}groupinfo
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN MENU
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Group Menu`;
        await sendImageWithCircleVideo(socket, sender, logo, groupMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '5') {
        const toolsMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *🛠️ TOOLS MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🖼️ Getdp:* ${prefix}getdp
┃ *ℹ️ About:* ${prefix}about 947xxxxxxxx
┃ *📰 Follow:* ${prefix}follow jid
┃ *🔔 Unfollow:* ${prefix}unfollow jid
┃ *❤️ React:* ${prefix}chr channel_post_link,emoji
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN MENU
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Tools Menu`;
        await sendImageWithCircleVideo(socket, sender, logo, toolsMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '6') {
        const eduMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *📚 EDUCATION MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📖 Coming Soon...*
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN MENU
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Education Menu`;
        await sendImageWithCircleVideo(socket, sender, logo, eduMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '7') {
        const channelMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *📺 CHANNEL MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🔗 Channel:* ${config.CHANNEL_LINK || 'Not set'}
┃ *👥 Group:* ${config.GROUP_INVITE_LINK || 'Not set'}
┃ *📰 Follow:* ${prefix}follow channel_jid
┃ *🔔 Unfollow:* ${prefix}unfollow
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN MENU
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Channel Menu`;
        await sendImageWithCircleVideo(socket, sender, logo, channelMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '0') {
        // Show main menu
        const mainMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *☠️ MAIN MENU☠️*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *👑 Owner:* ${config.OWNER_NAME}
┃ *📌 Version:* ${config.BOT_VERSION}
┃ *⚡ Commands:* 20+
┃ *🔧 Prefix:* [ ${prefix} ]
┃ *🟢 Active Bots:* ${activeSockets.size}
┃ *🎬 Media:* Circle Video
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *1* - OWNER MENU
┃ *2* - SOCIAL MENU
┃ *3* - AI MENU
┃ *4* - GROUP MENU
┃ *5* - TOOLS MENU
┃ *6* - EDUCATION MENU
┃ *7* - CHANNEL MENU
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Type ${prefix}setting for settings panel`;
        await sendImageWithCircleVideo(socket, sender, logo, mainMenu, botName);
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
      const metaQuote = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: `META_${Date.now()}` },
        message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nEND:VCARD` } }
      };

      // ==================== SETTINGS PANEL ====================
      if (command === 'setting') {
        const subCommand = args[0]?.toLowerCase();
        const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
        
        if (!subCommand || subCommand === 'menu') {
          const settingsPanel = `╭━━━━━━━━━━━━━❥❥❥
┃     *⚙️ SETTINGS PANEL*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *1* - Auto View Status: ${currentConfig.AUTO_VIEW_STATUS || config.AUTO_VIEW_STATUS}
┃ *2* - Auto Like Status: ${currentConfig.AUTO_LIKE_STATUS || config.AUTO_LIKE_STATUS}
┃ *3* - Auto Recording: ${currentConfig.AUTO_RECORDING || config.AUTO_RECORDING}
┃ *4* - Auto Typing: ${currentConfig.AUTO_TYPING || config.AUTO_TYPING}
┃ *5* - Auto Read Msg: ${currentConfig.AUTO_READ_MESSAGE || config.AUTO_READ_MESSAGE}
┃ *6* - Anti Call: ${currentConfig.ANTI_CALL || config.ANTI_CALL}
┃ *7* - Work Type: ${currentConfig.WORK_TYPE || config.WORK_TYPE}
┃ *8* - Delete Notify: ${currentConfig.DELETE_MESSAGE_NOTIFY || config.DELETE_MESSAGE_NOTIFY}
┃ *9* - Bot Name: ${currentConfig.botName || botName}
┃ *10* - Prefix: ${currentConfig.PREFIX || prefix}
┃ *0* - RESET ALL
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number to Toggle ⤵️ 」━╮
┃ *1-10* - Change setting
┃ *0* - Reset all
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Reply with number to change setting`;
          await sendImageWithCircleVideo(socket, sender, logo, settingsPanel, botName);
          numberReplyTracker.set(senderNumber, Date.now());
          return;
        }
        
        // Handle number replies for settings
        if (subCommand.match(/^[0-9]+$/)) {
          const option = subCommand;
          
          if (option === '1') {
            const newValue = currentConfig.AUTO_VIEW_STATUS === 'true' ? 'false' : 'true';
            currentConfig.AUTO_VIEW_STATUS = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto View Status: *${newValue === 'true' ? 'ON' : 'OFF'}*`, botName);
          }
          else if (option === '2') {
            const newValue = currentConfig.AUTO_LIKE_STATUS === 'true' ? 'false' : 'true';
            currentConfig.AUTO_LIKE_STATUS = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto Like Status: *${newValue === 'true' ? 'ON' : 'OFF'}*`, botName);
          }
          else if (option === '3') {
            const newValue = currentConfig.AUTO_RECORDING === 'true' ? 'false' : 'true';
            currentConfig.AUTO_RECORDING = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto Recording: *${newValue === 'true' ? 'ON' : 'OFF'}*`, botName);
          }
          else if (option === '4') {
            const newValue = currentConfig.AUTO_TYPING === 'true' ? 'false' : 'true';
            currentConfig.AUTO_TYPING = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto Typing: *${newValue === 'true' ? 'ON' : 'OFF'}*`, botName);
          }
          else if (option === '5') {
            const modes = ['all', 'cmd', 'off'];
            const currentIndex = modes.indexOf(currentConfig.AUTO_READ_MESSAGE || config.AUTO_READ_MESSAGE);
            const newIndex = (currentIndex + 1) % modes.length;
            currentConfig.AUTO_READ_MESSAGE = modes[newIndex];
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Auto Read Message: *${modes[newIndex]}*`, botName);
          }
          else if (option === '6') {
            const newValue = currentConfig.ANTI_CALL === 'on' ? 'off' : 'on';
            currentConfig.ANTI_CALL = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Anti Call: *${newValue === 'on' ? 'ON' : 'OFF'}*`, botName);
          }
          else if (option === '7') {
            const types = ['public', 'private', 'inbox', 'groups'];
            const currentIndex = types.indexOf(currentConfig.WORK_TYPE || config.WORK_TYPE);
            const newIndex = (currentIndex + 1) % types.length;
            currentConfig.WORK_TYPE = types[newIndex];
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Work Type: *${types[newIndex]}*`, botName);
          }
          else if (option === '8') {
            const newValue = currentConfig.DELETE_MESSAGE_NOTIFY === 'on' ? 'off' : 'on';
            currentConfig.DELETE_MESSAGE_NOTIFY = newValue;
            await setUserConfigInMongo(sanitized, currentConfig);
            await sendCircleVideo(socket, sender, `✅ Delete Message Notify: *${newValue === 'on' ? 'ON' : 'OFF'}*`, botName);
          }
          else if (option === '9') {
            await sendCircleVideo(socket, sender, `📝 Send new bot name as a reply:`, botName);
            const replyHandler = async (replyMsg) => {
              const replyBody = replyMsg.message?.conversation || replyMsg.message?.extendedTextMessage?.text;
              if (replyBody && replyMsg.key.remoteJid === sender && !replyBody.startsWith(prefix) && replyBody.length > 0 && replyBody.length < 50) {
                currentConfig.botName = replyBody;
                await setUserConfigInMongo(sanitized, currentConfig);
                await sendCircleVideo(socket, sender, `✅ Bot name changed to *${replyBody}*`, replyBody);
                socket.ev.off('messages.upsert', replyHandler);
              }
            };
            socket.ev.on('messages.upsert', replyHandler);
            setTimeout(() => socket.ev.off('messages.upsert', replyHandler), 30000);
          }
          else if (option === '10') {
            await sendCircleVideo(socket, sender, `📝 Send new prefix as a reply (single character):`, botName);
            const replyHandler = async (replyMsg) => {
              const replyBody = replyMsg.message?.conversation || replyMsg.message?.extendedTextMessage?.text;
              if (replyBody && replyMsg.key.remoteJid === sender && !replyBody.startsWith(prefix) && replyBody.length === 1) {
                currentConfig.PREFIX = replyBody;
                await setUserConfigInMongo(sanitized, currentConfig);
                await sendCircleVideo(socket, sender, `✅ Prefix changed to *${replyBody}*`, botName);
                socket.ev.off('messages.upsert', replyHandler);
              }
            };
            socket.ev.on('messages.upsert', replyHandler);
            setTimeout(() => socket.ev.off('messages.upsert', replyHandler), 30000);
          }
          else if (option === '0') {
            await setUserConfigInMongo(sanitized, {});
            await sendCircleVideo(socket, sender, `✅ *ALL SETTINGS RESET TO DEFAULT*`, botName);
          }
          return;
        }
        
        await sendCircleVideo(socket, sender, `❌ Unknown. Use ${prefix}setting or ${prefix}setting menu`, botName);
        return;
      }

      // ==================== OTHER COMMANDS ====================
      switch (command) {
        case 'alive': {
          const now = new Date();
          const sriLankaDate = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Colombo' }));
          const currentHour = sriLankaDate.getHours();
          let greeting = currentHour >= 5 && currentHour < 12 ? 'Good Morning 🌅' : currentHour >= 12 && currentHour < 18 ? 'Good Afternoon' : 'Good Evening 🌙';
          
          const startTime = socketCreationTime.get(number) || Date.now();
          const uptime = Math.floor((Date.now() - startTime) / 1000);
          const hours = Math.floor(uptime / 3600);
          const minutes = Math.floor((uptime % 3600) / 60);
          const seconds = Math.floor(uptime % 60);
          
          const aliveText = `╭━━━━━━━━━━━━━❥❥❥
┃     *🤖 BOT ALIVE*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🗯️ Greeting:* ${greeting}
┃ *🗓️ Date:* ${sriLankaDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric' })}
┃ *📆 Day:* ${sriLankaDate.toLocaleDateString('en-US', { weekday: 'long' })}
┃ *⏱️ Time:* ${sriLankaDate.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true })}
┃ *📄 Bot Name:* ${botName}
┃ *🥷 Owner:* ${config.OWNER_NAME}
┃ *🧬 Version:* ${config.BOT_VERSION}
┃ *📟 Uptime:* ${hours}h ${minutes}m ${seconds}s
┃ *✒️ Prefix:* ${prefix}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ ${botName} is Active!`;
          
          await sendImageWithCircleVideo(socket, sender, logo, aliveText, botName);
          break;
        }

        case 'ping': {
          const start = Date.now();
          const end = Date.now();
          const latency = end - start;
          const speedStatus = latency < 200 ? 'Excellent 🟢' : latency < 500 ? 'Good 🟡' : 'Slow 🔴';
          
          const pingText = `╭━━━━━━━━━━━━━❥❥❥
┃     *🏓 PONG!*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *⚡ Speed:* ${latency}ms
┃ *📡 Status:* ${speedStatus}
┃ *🤖 Bot:* ${botName}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Response Time: ${latency}ms`;
          
          await sendImageWithCircleVideo(socket, sender, logo, pingText, botName);
          break;
        }

        case 'menu':
        case 'help': {
          const mainMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *🏠 MAIN MENU*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *👑 Owner:* ${config.OWNER_NAME}
┃ *📌 Version:* ${config.BOT_VERSION}
┃ *⚡ Commands:* 20+
┃ *🔧 Prefix:* [ ${prefix} ]
┃ *🟢 Active Bots:* ${activeSockets.size}
┃ *🎬 Media:* Circle Video
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *1* - OWNER MENU
┃ *2* - SOCIAL MENU
┃ *3* - AI MENU
┃ *4* - GROUP MENU
┃ *5* - TOOLS MENU
┃ *6* - EDUCATION MENU
┃ *7* - CHANNEL MENU
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Type ${prefix}setting for settings panel`;
          
          await sendImageWithCircleVideo(socket, sender, logo, mainMenu, botName);
          numberReplyTracker.set(senderNumber, Date.now());
          break;
        }
const { cmd } = require('../command');
const { fetchJson } = require('../lib/functions');

const footer = "> © Powerd by lakshan md ☠️";
const menuImage = "https://raw.githubusercontent.com/Ranumithaofc/RANU-FILE-S-/refs/heads/main/images/GridArt_yellow.jpg";

let isChoosing = false;
let isChoosingQuality = false;

cmd({
    pattern: "xnxx",
    alias: ["xvdl", "xvideo", "phv"],
    use: ".xnxx <video name>",
    react: "🤤",
    desc: "Search & download xnxx.com videos (18+).",
    category: "download",
    filename: __filename
}, async (conn, mek, m, { q, from, reply }) => {

    try {
        if (!q) return await reply("❌ Please enter a video name!");

        isChoosing = false;
        isChoosingQuality = false;

        const searchApi = await fetchJson(
            `https://tharuzz-ofc-api-v2.vercel.app/api/search/xvsearch?query=${encodeURIComponent(q)}`
        );

        if (!searchApi.result?.xvideos?.length)
            return await reply("❌ No results found!");

        let listText = "lakshan-MD XNXX SEARCH RESULTS\n\n🔢 *Reply a number to choose a result.*\n\n";

        searchApi.result.xvideos.forEach((item, i) => {
            listText += `*${i + 1}.* | ${item.title || "No title"}\n`;
        });

        const listMsg = await conn.sendMessage(
            from,
            {
                image: { url: menuImage },
                caption: listText + `\n\n${footer}`
            },
            { quoted: mek }
        );

        const handleChoose = async (update) => {
            const msg = update.messages?.[0];
            if (!msg?.message) return;

            const txt =
                msg.message.conversation ||
                msg.message.extendedTextMessage?.text;

            const isReply =
                msg.message.extendedTextMessage?.contextInfo?.stanzaId === listMsg.key.id;

            if (!isReply) return;
            if (isChoosing) return; // 🔒 lock prevents duplicate triggers
            isChoosing = true;

            const index = parseInt(txt.trim()) - 1;

            if (isNaN(index) || index < 0 || index >= searchApi.result.xvideos.length) {
                isChoosing = false;
                return await reply("❌ Invalid number!");
            }

            const chosen = searchApi.result.xvideos[index];

            const downloadApi = await fetchJson(
                `https://tharuzz-ofc-api-v2.vercel.app/api/download/xvdl?url=${chosen.link}`
            );

            const info = downloadApi.result;
            const HQ = info.dl_Links.highquality;
            const LQ = info.dl_Links.lowquality;

            const askMsg = await conn.sendMessage(
                from,
                {
                    image: { url: info.thumbnail },
                    caption:
                        `*🔞 VIDEO INFO*\n\n` +
                        `*Title:* ${info.title}\n` +
                        `*Duration:* ${info.duration}\n\n` +
                        `Reply number:\n1 | High Quality\n2 | Low Quality\n\n${footer}`
                },
                { quoted: msg }
            );

            const handleQuality = async (u) => {
                const t = u.messages?.[0];
                if (!t?.message) return;

                const choice =
                    t.message.conversation ||
                    t.message.extendedTextMessage?.text;

                const isReplyQ =
                    t.message.extendedTextMessage?.contextInfo?.stanzaId === askMsg.key.id;

                if (!isReplyQ) return;
                if (isChoosingQuality) return; // 🔒 prevents double
                isChoosingQuality = true;

                let sendURL;

                if (choice.trim() === "1") sendURL = HQ;
                else if (choice.trim() === "2") sendURL = LQ;
                else {
                    isChoosingQuality = false;
                    return await reply("❌ Enter *1* or *2* only!");
                }

                // ⬇️ Download reaction
                await conn.sendMessage(from, {
                    react: { text: "⬇️", key: t.key }
                });

                // ⬆️ Upload reaction
                await conn.sendMessage(from, {
                    react: { text: "⬆️", key: t.key }
                });

                // Send Video
                await conn.sendMessage(
                    from,
                    {
                        video: { url: sendURL },
                        caption: `🔞 Video\n> ${info.title}`
                    },
                    { quoted: t }
                );

                // ✔️ Done reaction
                await conn.sendMessage(from, {
                    react: { text: "✔️", key: t.key }
                });

                isChoosing = false;
                isChoosingQuality = false;
            };

            conn.ev.on("messages.upsert", handleQuality);
        };

        conn.ev.on("messages.upsert", handleChoose);

    } catch (err) {
        console.log(err);
        await reply("❌ Error: " + err);
    }
});
case 'song': {
    const yts = require('yt-search');
    const axios = require('axios');

    // Extract YT video id & normalize link (reuse from original)
    function extractYouTubeId(url) {
        const regex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|embed\/|v\/|shorts\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;
        const match = url.match(regex);
        return match ? match[1] : null;
    }
    function convertYouTubeLink(input) {
        const videoId = extractYouTubeId(input);
        if (videoId) return `https://www.youtube.com/watch?v=${videoId}`;
        return input;
    }

    // get message text
    const q = msg.message?.conversation ||
        msg.message?.extendedTextMessage?.text ||
        msg.message?.imageMessage?.caption ||
        msg.message?.videoMessage?.caption || '';

    if (!q || q.trim() === '') {
        await socket.sendMessage(sender, { text: '*`Need YT_URL or Title`*' });
        break;
    }

    // load bot name
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || 'lakshan md';

    // fake contact for quoted card
    const botMention = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_AI_FAKE_ID_SONG"
        },
        message: {
            contactMessage: {
                displayName: botName,
                vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
            }
        }
    };

    try {
        // Determine video URL: if q contains YT id/url, use it; otherwise search by title
        let videoUrl = null;
        const maybeLink = convertYouTubeLink(q.trim());
        if (extractYouTubeId(q.trim())) {
            videoUrl = maybeLink;
        } else {
            // search by title
            const search = await yts(q.trim());
            const first = (search?.videos || [])[0];
            if (!first) {
                await socket.sendMessage(sender, { text: '*`No results found for that title`*' }, { quoted: botMention });
                break;
            }
            videoUrl = first.url;
        }

        // call your mp3 API (the one you provided)
        const apiUrl = `https://api.srihub.store/download/ytmp3?apikey=dew_EtVuyJGtlCzvZY44TP6MbXpPlAltC6VH2uGOPAJL&url=https%3A%2F%2Fyoutu.be%2FajdRPlWnuUM%3Fsi%3DeO5_cnVYLb9jzgaa${encodeURIComponent(videoUrl)}`;
        const apiRes = await axios.get(apiUrl, { timeout: 15000 }).then(r => r.data).catch(e => null);

        if (!apiRes || (!apiRes.downloadUrl && !apiRes.result?.download?.url && !apiRes.result?.url)) {
            await socket.sendMessage(sender, { text: '*`MP3 API returned no download link`*' }, { quoted: botMention });
            break;
        }

        // Normalize download URL and metadata
        const downloadUrl = apiRes.downloadUrl || apiRes.result?.download?.url || apiRes.result?.url;
        const title = apiRes.title || apiRes.result?.title || 'Unknown title';
        const thumb = apiRes.thumbnail || apiRes.result?.thumbnail || null;
        const duration = apiRes.duration || apiRes.result?.duration || null;
        const quality = apiRes.quality || apiRes.result?.quality || '128';

        const caption = `🎵 *Title:* ${title}
⏱️ *Duration:* ${duration || 'N/A'}
🔊 *Quality:* ${quality}
🔗 *Source:* ${videoUrl}

*Reply to this message (quote it) with a number to choose format:*
01. 📄 MP3 as Document
02. 🎧 MP3 as Audio
03. 🎙 MP3 as Voice Note (PTT)

_© Powered by ${botName}_`;

        // send thumbnail card if available
        const sendOpts = { quoted: botMention };
        const media = thumb ? { image: { url: thumb }, caption } : { text: caption };
        const resMsg = await socket.sendMessage(sender, media, sendOpts);

        // handler waits for quoted reply from same sender
        const handler = async (msgUpdate) => {
            try {
                const received = msgUpdate.messages && msgUpdate.messages[0];
                if (!received) return;

                const fromId = received.key.remoteJid || received.key.participant || (received.key.fromMe && sender);
                if (fromId !== sender) return;

                const text = received.message?.conversation || received.message?.extendedTextMessage?.text;
                if (!text) return;

                // ensure they quoted our card
                const quotedId = received.message?.extendedTextMessage?.contextInfo?.stanzaId ||
                    received.message?.extendedTextMessage?.contextInfo?.quotedMessage?.key?.id;
                if (!quotedId || quotedId !== resMsg.key.id) return;

                const choice = text.toString().trim().split(/\s+/)[0];

                await socket.sendMessage(sender, { react: { text: "📥", key: received.key } });

                switch (choice) {
                    case "1":
                        await socket.sendMessage(sender, {
                            document: { url: downloadUrl },
                            mimetype: "audio/mpeg",
                            fileName: `${title}.mp3`
                        }, { quoted: received });
                        break;
                    case "2":
                        await socket.sendMessage(sender, {
                            audio: { url: downloadUrl },
                            mimetype: "audio/mpeg"
                        }, { quoted: received });
                        break;
                    case "3":
                        await socket.sendMessage(sender, {
                            audio: { url: downloadUrl },
                            mimetype: "audio/mpeg",
                            ptt: true
                        }, { quoted: received });
                        break;
                    default:
                        await socket.sendMessage(sender, { text: "*Invalid option. Reply with 1, 2 or 3 (quote the card).*" }, { quoted: received });
                        return;
                }

                // cleanup listener after successful send
                socket.ev.off('messages.upsert', handler);
            } catch (err) {
                console.error("Song handler error:", err);
                try { socket.ev.off('messages.upsert', handler); } catch (e) {}
            }
        };

        socket.ev.on('messages.upsert', handler);

        // auto-remove handler after 60s
        setTimeout(() => {
            try { socket.ev.off('messages.upsert', handler); } catch (e) {}
        }, 60 * 1000);

        // react to original command
        await socket.sendMessage(sender, { react: { text: '🔎', key: msg.key } });

    } catch (err) {
        console.error('Song case error:', err);
        await socket.sendMessage(sender, { text: "*`Error occurred while processing song request`*" }, { quoted: botMention });
    }

    break;
}
 
        case 'system': {
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
╰━━━━━━━━━━━━━❥❥❥

> ☠️ System Information`;
          
          await sendImageWithCircleVideo(socket, sender, logo, sysText, botName);
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
            await sendCircleVideo(socket, sender, `❌ No profile picture found for ${targetJid.split('@')[0]}`, botName);
          }
          break;
        }

        case 'about': {
          const targetNumber = args[0]?.replace(/[^0-9]/g, '');
          if (!targetNumber) {
            await sendCircleVideo(socket, sender, `❌ Usage: ${prefix}about 947xxxxxxxx`, botName);
            break;
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
┃ *📅 Set at:* ${statusData.setAt ? moment(statusData.setAt).tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss') : 'Unknown'}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ User Information`;
            await sendCircleVideo(socket, sender, aboutText, botName);
          } catch (error) {
            await sendCircleVideo(socket, sender, `❌ Failed to fetch about for ${targetNumber}`, botName);
          }
          break;
        }

        // ==================== REACTION COMMAND (chr) ====================
        case 'chr': {
          const q = args.join(' ').trim();
          if (!q.includes(',')) {
            await sendCircleVideo(socket, sender, `❌ Usage: ${prefix}chr channel_post_link,emoji\n\nExample: ${prefix}chr https://whatsapp.com/channel/xxxxx/123,❤️`, botName);
            break;
          }

          const parts = q.split(',');
          let channelLink = parts[0].trim();
          const reactEmoji = parts[1].trim();

          // Extract channel ID and message ID from link
          let channelJid = null;
          let messageId = null;
          
          // Pattern: https://whatsapp.com/channel/CHANNEL_ID/MESSAGE_ID
          const linkMatch = channelLink.match(/channel\/([a-zA-Z0-9]+)\/([a-zA-Z0-9]+)/);
          if (linkMatch) {
            channelJid = `${linkMatch[1]}@newsletter`;
            messageId = linkMatch[2];
          } else {
            // Try direct format: JID/MessageID
            const directMatch = channelLink.match(/(\d+@newsletter)\/(.+)/);
            if (directMatch) {
              channelJid = directMatch[1];
              messageId = directMatch[2];
            }
          }

          if (!channelJid || !messageId) {
            await sendCircleVideo(socket, sender, `❌ Invalid channel post link.\n\nUse: ${prefix}chr https://whatsapp.com/channel/xxxxx/123,❤️`, botName);
            break;
          }

          try {
            if (typeof socket.newsletterReactMessage === 'function') {
              await socket.newsletterReactMessage(channelJid, messageId.toString(), reactEmoji);
              await saveNewsletterReaction(channelJid, messageId.toString(), reactEmoji, sanitized);
              
              const reactText = `╭━━━━━━━━━━━━━❥❥❥
┃     *✅ REACTION SUCCESS*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📺 Channel:* ${channelJid}
┃ *🆔 Message:* ${messageId}
┃ *❤️ Emoji:* ${reactEmoji}
┃ *👤 By:* @${senderNumber}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Reaction added successfully!`;
              
              await sendCircleVideo(socket, sender, reactText, botName, [nowsender]);
            } else {
              await sendCircleVideo(socket, sender, `❌ Newsletter reaction not supported`, botName);
            }
          } catch (e) {
            await sendCircleVideo(socket, sender, `❌ Failed to react: ${e.message || e}`, botName);
          }
          break;
        }

        // ==================== GROUP COMMANDS ====================
        case 'kick': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName); break; }
          if (!(await isBotAdmin(socket, from))) { await sendCircleVideo(socket, sender, `❌ Bot not admin`, botName); break; }
          
          const mentioned = msg.message.extendedTextMessage?.contextInfo?.mentionedJid || [];
          if (mentioned.length === 0) { await sendCircleVideo(socket, sender, `❌ Tag user to kick`, botName); break; }
          
          for (const user of mentioned) {
            if (user === `${botNumber}@s.whatsapp.net`) continue;
            await socket.groupParticipantsUpdate(from, [user], 'remove');
          }
          await sendCircleVideo(socket, sender, `✅ Kicked ${mentioned.length} user(s)`, botName);
          break;
        }

        case 'add': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName); break; }
          if (!(await isBotAdmin(socket, from))) { await sendCircleVideo(socket, sender, `❌ Bot not admin`, botName); break; }
          
          const numberToAdd = args[0]?.replace(/[^0-9]/g, '');
          if (!numberToAdd) { await sendCircleVideo(socket, sender, `❌ Provide number: ${prefix}add 947xxxxxxxx`, botName); break; }
          
          await socket.groupParticipantsUpdate(from, [`${numberToAdd}@s.whatsapp.net`], 'add');
          await sendCircleVideo(socket, sender, `✅ Added ${numberToAdd}`, botName);
          break;
        }

        case 'promote': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName); break; }
          if (!(await isBotAdmin(socket, from))) { await sendCircleVideo(socket, sender, `❌ Bot not admin`, botName); break; }
          
          const mentioned = msg.message.extendedTextMessage?.contextInfo?.mentionedJid || [];
          if (mentioned.length === 0) { await sendCircleVideo(socket, sender, `❌ Tag user to promote`, botName); break; }
          
          for (const user of mentioned) await socket.groupParticipantsUpdate(from, [user], 'promote');
          await sendCircleVideo(socket, sender, `✅ Promoted ${mentioned.length} user(s)`, botName);
          break;
        }

        case 'demote': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName); break; }
          if (!(await isBotAdmin(socket, from))) { await sendCircleVideo(socket, sender, `❌ Bot not admin`, botName); break; }
          
          const mentioned = msg.message.extendedTextMessage?.contextInfo?.mentionedJid || [];
          if (mentioned.length === 0) { await sendCircleVideo(socket, sender, `❌ Tag user to demote`, botName); break; }
          
          for (const user of mentioned) await socket.groupParticipantsUpdate(from, [user], 'demote');
          await sendCircleVideo(socket, sender, `✅ Demoted ${mentioned.length} user(s)`, botName);
          break;
        }

        case 'tagall': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName); break; }
          if (!isOwner && !(await isUserAdmin(socket, from, nowsender))) { await sendCircleVideo(socket, sender, `❌ Admin only`, botName); break; }
          
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
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName); break; }
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName); break; }
          
          await sendCircleVideo(socket, sender, `👋 Goodbye!`, botName);
          await delay(2000);
          await socket.groupLeave(from);
          break;
        }

        case 'groupinfo': {
          if (!isGroup) { await sendCircleVideo(socket, sender, `❌ Group only command`, botName); break; }
          
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
          await sendCircleVideo(socket, sender, infoText, botName, adminMentions);
          break;
        }

        // ==================== ADMIN COMMANDS ====================
        case 'addadmin': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName); break; }
          const adminJid = args[0];
          if (!adminJid) { await sendCircleVideo(socket, sender, `❌ Provide JID: ${prefix}addadmin 947xxxxxxxx`, botName); break; }
          const formattedJid = adminJid.includes('@') ? adminJid : `${adminJid.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
          await addAdminToMongo(formattedJid);
          await sendCircleVideo(socket, sender, `✅ Added ${formattedJid} as admin`, botName);
          break;
        }

        case 'removeadmin': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName); break; }
          const adminJid = args[0];
          if (!adminJid) { await sendCircleVideo(socket, sender, `❌ Provide JID`, botName); break; }
          const formattedJid = adminJid.includes('@') ? adminJid : `${adminJid.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
          await removeAdminFromMongo(formattedJid);
          await sendCircleVideo(socket, sender, `✅ Removed ${formattedJid} from admins`, botName);
          break;
        }

        case 'listadmin': {
          const admins = await loadAdminsFromMongo();
          if (admins.length === 0) {
            await sendCircleVideo(socket, sender, `📋 No admins found`, botName);
          } else {
            let adminList = `📋 *ADMIN LIST*\n\n`;
            admins.forEach((admin, i) => { adminList += `${i + 1}. ${admin}\n`; });
            await sendCircleVideo(socket, sender, adminList, botName);
          }
          break;
        }

        // ==================== NEWSLETTER COMMANDS ====================
        case 'follow': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName); break; }
          const newsletterJid = args[0];
          if (!newsletterJid) { await sendCircleVideo(socket, sender, `❌ Provide JID: ${prefix}follow 120363334838967293@newsletter`, botName); break; }
          await addNewsletterToMongo(newsletterJid, args.slice(1));
          if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(newsletterJid);
          await sendCircleVideo(socket, sender, `✅ Following: ${newsletterJid}`, botName);
          break;
        }

        case 'unfollow': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName); break; }
          const newsletterJid = args[0];
          if (!newsletterJid) { await sendCircleVideo(socket, sender, `❌ Provide JID`, botName); break; }
          await removeNewsletterFromMongo(newsletterJid);
          if (typeof socket.newsletterUnfollow === 'function') await socket.newsletterUnfollow(newsletterJid);
          await sendCircleVideo(socket, sender, `✅ Unfollowed: ${newsletterJid}`, botName);
          break;
        }

        // ==================== OWNER COMMANDS ====================
        case 'shutdown': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName); break; }
          await sendCircleVideo(socket, sender, `🔄 Shutting down...`, botName);
          process.exit(0);
          break;
        }

        case 'restart': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName); break; }
          await sendCircleVideo(socket, sender, `🔄 Restarting...`, botName);
          exec(`pm2 restart ${process.env.PM2_NAME || 'CHATUWA-MINI-main'}`);
          break;
        }

        case 'broadcast': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName); break; }
          const broadcastMsg = args.join(' ');
          if (!broadcastMsg) { await sendCircleVideo(socket, sender, `❌ Provide message to broadcast`, botName); break; }
          
          const allNumbers = await getAllNumbersFromMongo();
          let successCount = 0;
          for (const num of allNumbers) {
            const sock = activeSockets.get(num);
            if (sock) {
              try {
                const userJid = jidNormalizedUser(sock.user.id);
                await sendCircleVideo(sock, userJid, `📢 *BROADCAST*\n\n${broadcastMsg}`, botName);
                successCount++;
              } catch (e) {}
            }
          }
          await sendCircleVideo(socket, sender, `✅ Broadcast sent to ${successCount} sessions`, botName);
          break;
        }

        default:
          break;
      }
    } catch (err) {
      console.error('Command error:', err);
      try {
        await sendCircleVideo(socket, sender, `❌ Error: ${err.message}`, BOT_NAME_FANCY);
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
          
          const welcomeText = `╭━━━━━━━━━━━━━❥❥❥
┃     *✅ BOT CONNECTED*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📞 Number:* ${sanitizedNumber}
┃ *🏠 Group:* ${groupResult.status === 'success' ? '✅ Joined' : '❌ Failed'}
┃ *📰 Channel:* ${channelResult.status === 'success' ? '✅ Followed' : '❌ Failed'}
┃ *🕒 Time:* ${getSriLankaTimestamp()}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Bot is now active! Type .menu to start`;
          
          await sendCircleVideo(socket, userJid, welcomeText, BOT_NAME_FANCY);
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