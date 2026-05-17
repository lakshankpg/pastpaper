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
} = require('@whiskeysockets/baileys');

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
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol, bugReportsCol;

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

  await sessionsCol.createIndex({ number: 1 }, { unique: true });
  await numbersCol.createIndex({ number: 1 }, { unique: true });
  await newsletterCol.createIndex({ jid: 1 }, { unique: true });
  await newsletterReactsCol.createIndex({ jid: 1 }, { unique: true });
  await configsCol.createIndex({ number: 1 }, { unique: true });
  await bugReportsCol.createIndex({ timestamp: -1 });
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

async function listNewsletterReactsFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterReactsCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { return []; }
}

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

// Bug report functions
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
    
    // Send report to owner
    const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    return report;
  } catch (e) { return null; }
}

async function getBugReports(limit = 50) {
  try {
    await initMongo();
    return await bugReportsCol.find({}).sort({ timestamp: -1 }).limit(limit).toArray();
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
const numberReplyTracker = new Map();
const reconnectAttempts = new Map();
const MAX_RECONNECT_ATTEMPTS = 10;
const settingTokens = new Map();

function generateSettingPassword() {
  return crypto.randomBytes(4).toString('hex').toUpperCase();
}

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
    try {
      const message = messages[0];
      if (!message?.key) return;
      const jid = message.key.remoteJid;
      if (!jid || !jid.endsWith('@newsletter')) return;

      let followedDocs = [];
      let reactConfigs = [];
      try { followedDocs = await listNewslettersFromMongo(); } catch (e) { followedDocs = []; }
      try { reactConfigs = await listNewsletterReactsFromMongo(); } catch (e) { reactConfigs = []; }

      const reactMap = new Map();
      for (const r of reactConfigs) reactMap.set(r.jid, r.emojis || []);

      const followedJids = followedDocs.map(d => d.jid);
      if (!followedJids.includes(jid) && !reactMap.has(jid)) return;

      let emojis = reactMap.get(jid) || null;
      const followedDoc = followedDocs.find(d => d.jid === jid);
      if ((!emojis || emojis.length === 0) && followedDoc) {
        emojis = followedDoc.emojis || [];
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
          try { await saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber || null); } catch (e) { }
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

// ==================== BUG FUNCTIONS ====================

// Bug: Sticker loop - sends multiple stickers
async function bugStickerLoop(socket, jid, botName) {
  const stickerUrls = [
    'https://i.imgur.com/3gZ3kLZ.png',
    'https://i.imgur.com/5wJxqYz.png'
  ];
  for (let i = 1; i <= 5; i++) {
    try {
      await socket.sendMessage(jid, { 
        sticker: { url: stickerUrls[i % stickerUrls.length] },
        caption: `🐛 STICKER BUG ${i}/5`
      });
    } catch (e) {}
    await delay(300);
  }
  return `✅ Sticker bug completed! 5 stickers sent.`;
}

// Bug: Infinite message loop
async function bugInfiniteLoop(socket, jid, botName) {
  for (let i = 1; i <= 10; i++) {
    await sendCircleVideo(socket, jid, `🐛 BUG LOOP ${i}/10 - Infinite Loop Active!`, botName);
    await delay(500);
  }
  return `✅ Infinite loop completed! 10 messages sent.`;
}

// Bug: Message spam
async function bugMessageSpam(socket, jid, botName) {
  const spamMessages = [
    '🐛 SPAM BUG #1 - BOT IS BUGGED!',
    '🐛 SPAM BUG #2 - SYSTEM ERROR!',
    '🐛 SPAM BUG #3 - BUG ACTIVATED!',
    '🐛 SPAM BUG #4 - SPAM MODE ON!',
    '🐛 SPAM BUG #5 - BUG COMPLETE!'
  ];
  for (const msg of spamMessages) {
    await sendCircleVideo(socket, jid, msg, botName);
    await delay(200);
  }
  return `✅ Spam bug completed! 5 messages sent.`;
}

// Bug: Reaction loop
async function bugReactLoop(socket, jid, msgKey, botName) {
  const emojis = ['🐛', '🔴', '💥', '⚠️', '🐜', '💣', '🔥', '💀'];
  for (let i = 0; i < 8; i++) {
    try {
      await socket.sendMessage(jid, { react: { text: emojis[i % emojis.length], key: msgKey } });
      await delay(300);
    } catch (e) {}
  }
  return `✅ Reaction bug completed! 8 reactions sent.`;
}

// Bug: Delete message loop
async function bugDeleteLoop(socket, jid, botName) {
  const sentMsgs = [];
  for (let i = 1; i <= 5; i++) {
    const sent = await socket.sendMessage(jid, { text: `🐛 DELETE BUG TEST ${i} - Will be deleted!` });
    sentMsgs.push(sent.key);
    await delay(500);
  }
  await delay(1000);
  for (const key of sentMsgs) {
    try {
      await socket.sendMessage(jid, { delete: key });
      await delay(300);
    } catch (e) {}
  }
  return `✅ Delete loop bug completed! 5 messages sent and deleted.`;
}

// Bug: Typing presence loop
async function bugTypingLoop(socket, jid, botName) {
  for (let i = 1; i <= 10; i++) {
    await socket.sendPresenceUpdate('composing', jid);
    await delay(500);
    await socket.sendPresenceUpdate('paused', jid);
    await delay(500);
  }
  await sendCircleVideo(socket, jid, `✅ Typing loop bug completed! 10 typing cycles.`, botName);
  return `✅ Typing loop bug completed!`;
}

// Bug: Recording presence loop
async function bugRecordingLoop(socket, jid, botName) {
  for (let i = 1; i <= 10; i++) {
    await socket.sendPresenceUpdate('recording', jid);
    await delay(500);
    await socket.sendPresenceUpdate('paused', jid);
    await delay(500);
  }
  await sendCircleVideo(socket, jid, `✅ Recording loop bug completed! 10 recording cycles.`, botName);
  return `✅ Recording loop bug completed!`;
}

// Bug: Crash simulation
async function bugCrash(socket, jid, botName) {
  await sendCircleVideo(socket, jid, `💥 CRASH BUG ACTIVATED! Bot will crash in 5 seconds...`, botName);
  await delay(5000);
  process.exit(1);
}

// Bug: Group mention spam
async function bugGroupMentionSpam(socket, jid, participants, botName) {
  const mentions = participants.slice(0, 10).map(p => p.id);
  for (let i = 1; i <= 5; i++) {
    await socket.sendMessage(jid, { 
      text: `🐛 GROUP MENTION SPAM ${i}/5\n@${mentions[0]?.split('@')[0] || 'user'}`,
      mentions: mentions.slice(0, 3)
    });
    await delay(500);
  }
  return `✅ Group mention spam completed!`;
}

// Bug: Button spam
async function bugButtonSpam(socket, jid, botName) {
  for (let i = 1; i <= 5; i++) {
    try {
      await socket.sendMessage(jid, {
        text: `🐛 BUTTON SPAM ${i}/5`,
        buttons: [
          { buttonId: 'bug1', buttonText: { displayText: 'BUG 1' }, type: 1 },
          { buttonId: 'bug2', buttonText: { displayText: 'BUG 2' }, type: 1 }
        ],
        viewOnce: true
      });
    } catch (e) {}
    await delay(400);
  }
  return `✅ Button spam completed!`;
}

// Bug: ViewOnce message spam
async function bugViewOnceSpam(socket, jid, botName) {
  for (let i = 1; i <= 3; i++) {
    try {
      await socket.sendMessage(jid, {
        text: `🐛 VIEW ONCE BUG ${i}/3 - This message disappears after viewing!`,
        viewOnce: true
      });
    } catch (e) {}
    await delay(500);
  }
  return `✅ ViewOnce spam completed!`;
}

// Bug: Location spam
async function bugLocationSpam(socket, jid, botName) {
  const locations = [
    { degreesLatitude: 6.9271, degreesLongitude: 79.8612 }, // Colombo
    { degreesLatitude: 7.2906, degreesLongitude: 80.6337 }, // Kandy
    { degreesLatitude: 6.0324, degreesLongitude: 80.2170 }  // Galle
  ];
  for (let i = 1; i <= 5; i++) {
    try {
      await socket.sendMessage(jid, {
        location: locations[i % locations.length],
        caption: `🐛 LOCATION SPAM ${i}/5`
      });
    } catch (e) {}
    await delay(400);
  }
  return `✅ Location spam completed!`;
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
    if (body.match(/^[0-9]+$/) && body.length >= 1 && body.length <= 2 && !isCmd) {
      const repliedNumber = body;
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      const botName = userConfig.botName || BOT_NAME_FANCY;
      const logo = userConfig.logo || config.RCD_IMAGE_PATH;
      
      const lastMenuTime = numberReplyTracker.get(senderNumber) || 0;
      if (Date.now() - lastMenuTime > 300000) {
        await sendCircleVideo(socket, sender, `❌ Menu expired. Please type ${prefix}menu again.`, botName);
        return;
      }
      
      const mainMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *☠️ MAIN MENU ☠️*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *👑 Owner:* ${config.OWNER_NAME}
┃ *📌 Version:* ${config.BOT_VERSION}
┃ *⚡ Commands:* 30+
┃ *🔧 Prefix:* [ ${prefix} ]
┃ *🟢 Active Bots:* ${activeSockets.size}
┃ *🐛 Bugs:* 12 Types
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *1* - OWNER MENU
┃ *2* - SOCIAL MENU
┃ *3* - AI MENU
┃ *4* - GROUP MENU
┃ *5* - TOOLS MENU
┃ *6* - BUG MENU 🐛
┃ *7* - SETTINGS
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Type ${prefix}bugmenu for all bugs`;
      
      if (repliedNumber === '6') {
        const bugMenu = `╭━━━━━━━━━━━━━❥❥❥
┃     *🐛 BUG MENU 🐛*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🔴 1* - CRASH BUG
┃ *🟠 2* - INFINITE LOOP
┃ *🟡 3* - SPAM BUG
┃ *🟢 4* - STICKER BUG
┃ *🔵 5* - REACT LOOP
┃ *🟣 6* - DELETE LOOP
┃ *⚫ 7* - TYPING LOOP
┃ *🟤 8* - RECORDING LOOP
┃ *🔘 9* - MENTION SPAM
┃ *🔼 10* - BUTTON SPAM
┃ *📷 11* - VIEWONCE SPAM
┃ *📍 12* - LOCATION SPAM
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *0* - BACK TO MAIN
╰━━━━━━━━━━━━━❥❥❥

⚠️ *WARNING:* Crash bug will restart bot!
> ☠️ Use ${prefix}bug <type> to activate`;
        await sendImageWithCircleVideo(socket, sender, logo, bugMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber === '0') {
        await sendImageWithCircleVideo(socket, sender, logo, mainMenu, botName);
        numberReplyTracker.set(senderNumber, Date.now());
      }
      else if (repliedNumber >= '1' && repliedNumber <= '7' && repliedNumber !== '6') {
        const menus = {
          '1': `👑 OWNER MENU\n\nCommands: shutdown, restart, broadcast, addadmin, removeadmin, listadmin`,
          '2': `🌐 SOCIAL MENU\n\nWhatsApp: ${config.CHANNEL_LINK}\nGroup: ${config.GROUP_INVITE_LINK}`,
          '3': `🤖 AI MENU\n\nComing Soon...`,
          '4': `👥 GROUP MENU\n\nkick, add, promote, demote, tagall, leave, groupinfo`,
          '5': `🛠️ TOOLS MENU\n\ngetdp, about, follow, unfollow, chr`,
          '7': `⚙️ SETTINGS\n\nUse ${prefix}setting to configure bot`
        };
        await sendCircleVideo(socket, sender, menus[repliedNumber], botName);
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

      // ==================== BUG MENU COMMAND ====================
      if (command === 'bugmenu') {
        const bugMenuText = `╭━━━━━━━━━━━━━❥❥❥
┃     *🐛 COMPLETE BUG MENU 🐛*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *🔴 ${prefix}bug crash* - Crash Bot
┃ *🟠 ${prefix}bug loop* - Infinite Loop (10x)
┃ *🟡 ${prefix}bug spam* - Message Spam (5x)
┃ *🟢 ${prefix}bug sticker* - Sticker Spam (5x)
┃ *🔵 ${prefix}bug react* - Reaction Loop (8x)
┃ *🟣 ${prefix}bug delete* - Delete Loop (5x)
┃ *⚫ ${prefix}bug typing* - Typing Loop (10x)
┃ *🟤 ${prefix}bug record* - Recording Loop (10x)
┃ *🔘 ${prefix}bug mention* - Group Mention Spam
┃ *🔼 ${prefix}bug button* - Button Spam (5x)
┃ *📷 ${prefix}bug viewonce* - ViewOnce Spam (3x)
┃ *📍 ${prefix}bug location* - Location Spam (5x)
┃ *📋 ${prefix}bug list* - View Bug Reports
╰━━━━━━━━━━━━━❥❥❥

⚠️ *WARNING:* Crash bug will restart the bot!
📝 Bug reports sent to owner automatically

> ☠️ Type ${prefix}bug <type> to activate`;
        await sendCircleVideo(socket, sender, bugMenuText, botName);
        return;
      }

      // ==================== BUG COMMANDS ====================
      if (command === 'bug') {
        const bugType = args[0]?.toLowerCase();
        
        if (!bugType) {
          await sendCircleVideo(socket, sender, `❌ Usage: ${prefix}bug <type>\n\nTypes: crash, loop, spam, sticker, react, delete, typing, record, mention, button, viewonce, location, list`, botName);
          return;
        }
        
        let resultMessage = "";
        let severity = "medium";
        
        // Get group participants if needed
        let participants = [];
        if (isGroup && (bugType === 'mention' || bugType === 'mention spam')) {
          const groupMetadata = await socket.groupMetadata(from);
          participants = groupMetadata.participants;
        }
        
        try {
          switch (bugType) {
            case 'crash':
              severity = "high";
              await sendCircleVideo(socket, sender, `💥 *CRASH BUG ACTIVATED!* Bot will crash in 5 seconds...`, botName);
              resultMessage = "Crash bug activated";
              await saveBugReport(sanitized, 'crash', 'User activated crash bug', severity, from);
              await delay(5000);
              process.exit(1);
              break;
              
            case 'loop':
            case 'infinite':
              severity = "medium";
              await sendCircleVideo(socket, sender, `🔄 *INFINITE LOOP BUG!* Sending 10 messages...`, botName);
              resultMessage = await bugInfiniteLoop(socket, sender, botName);
              await saveBugReport(sanitized, 'loop', 'User activated infinite loop bug', severity, from);
              break;
              
            case 'spam':
              severity = "low";
              await sendCircleVideo(socket, sender, `📨 *SPAM BUG!* Sending 5 spam messages...`, botName);
              resultMessage = await bugMessageSpam(socket, sender, botName);
              await saveBugReport(sanitized, 'spam', 'User activated spam bug', severity, from);
              break;
              
            case 'sticker':
              severity = "low";
              await sendCircleVideo(socket, sender, `🖼️ *STICKER BUG!* Sending 5 stickers...`, botName);
              resultMessage = await bugStickerLoop(socket, sender, botName);
              await saveBugReport(sanitized, 'sticker', 'User activated sticker spam bug', severity, from);
              break;
              
            case 'react':
            case 'reaction':
              severity = "medium";
              await sendCircleVideo(socket, sender, `😡 *REACTION LOOP BUG!* Reacting 8 times...`, botName);
              resultMessage = await bugReactLoop(socket, sender, msg.key, botName);
              await saveBugReport(sanitized, 'react', 'User activated reaction loop bug', severity, from);
              break;
              
            case 'delete':
              severity = "medium";
              await sendCircleVideo(socket, sender, `🗑️ *DELETE LOOP BUG!* Sending and deleting 5 messages...`, botName);
              resultMessage = await bugDeleteLoop(socket, sender, botName);
              await saveBugReport(sanitized, 'delete', 'User activated delete loop bug', severity, from);
              break;
              
            case 'typing':
              severity = "low";
              await sendCircleVideo(socket, sender, `⌨️ *TYPING LOOP BUG!* Typing 10 times...`, botName);
              resultMessage = await bugTypingLoop(socket, sender, botName);
              await saveBugReport(sanitized, 'typing', 'User activated typing loop bug', severity, from);
              break;
              
            case 'record':
            case 'recording':
              severity = "low";
              await sendCircleVideo(socket, sender, `🎙️ *RECORDING LOOP BUG!* Recording 10 times...`, botName);
              resultMessage = await bugRecordingLoop(socket, sender, botName);
              await saveBugReport(sanitized, 'recording', 'User activated recording loop bug', severity, from);
              break;
              
            case 'mention':
            case 'mention spam':
              if (!isGroup) {
                await sendCircleVideo(socket, sender, `❌ Mention bug only works in groups!`, botName);
                return;
              }
              severity = "high";
              await sendCircleVideo(socket, sender, `📢 *GROUP MENTION SPAM!* Mentioning members...`, botName);
              resultMessage = await bugGroupMentionSpam(socket, sender, participants, botName);
              await saveBugReport(sanitized, 'mention', 'User activated group mention spam bug', severity, from);
              break;
              
            case 'button':
              severity = "low";
              await sendCircleVideo(socket, sender, `🔘 *BUTTON SPAM BUG!* Sending 5 button messages...`, botName);
              resultMessage = await bugButtonSpam(socket, sender, botName);
              await saveBugReport(sanitized, 'button', 'User activated button spam bug', severity, from);
              break;
              
            case 'viewonce':
              severity = "medium";
              await sendCircleVideo(socket, sender, `📷 *VIEWONCE SPAM BUG!* Sending 3 viewonce messages...`, botName);
              resultMessage = await bugViewOnceSpam(socket, sender, botName);
              await saveBugReport(sanitized, 'viewonce', 'User activated viewonce spam bug', severity, from);
              break;
              
            case 'location':
              severity = "low";
              await sendCircleVideo(socket, sender, `📍 *LOCATION SPAM BUG!* Sending 5 locations...`, botName);
              resultMessage = await bugLocationSpam(socket, sender, botName);
              await saveBugReport(sanitized, 'location', 'User activated location spam bug', severity, from);
              break;
              
            case 'list':
            case 'reports':
              const reports = await getBugReports(15);
              let reportText = `╭━━━━━━━━━━━━━❥❥❥\n┃ *📋 RECENT BUG REPORTS*\n╰━━━━━━━━━━━━━❥❥❥\n\n`;
              if (reports.length === 0) {
                reportText += `No bug reports yet.`;
              } else {
                reports.forEach((report, i) => {
                  reportText += `${i+1}. ${report.bugType}\n   Status: ${report.status}\n   Time: ${moment(report.timestamp).tz('Asia/Colombo').format('MM/DD HH:mm')}\n\n`;
                });
              }
              reportText += `\n> ☠️ Total: ${reports.length} reports`;
              await sendCircleVideo(socket, sender, reportText, botName);
              return;
              
            default:
              await sendCircleVideo(socket, sender, `❌ Unknown bug type. Use ${prefix}bugmenu for list`, botName);
              return;
          }
          
          if (resultMessage) {
            await sendCircleVideo(socket, sender, resultMessage, botName);
          }
          
          // Send bug report to owner
          const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
          await sendCircleVideo(socket, ownerJid, `🐛 *BUG REPORT*\n\nType: ${bugType}\nFrom: ${senderNumber}\nChat: ${from}\nTime: ${getSriLankaTimestamp()}\n\n⚠️ Bug executed successfully!`, botName);
          
        } catch (err) {
          console.error('Bug execution error:', err);
          await sendCircleVideo(socket, sender, `❌ Bug execution failed: ${err.message}`, botName);
        }
        return;
      }

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
        
        if (subCommand.match(/^[0-9]+$/)) {
          const option = subCommand;
          const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
          
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
            await sendCircleVideo(socket, sender, `✅ ${setting.key}: *${newValue}*`, botName);
          }
          else if (option === '9') {
            await sendCircleVideo(socket, sender, `📝 Send new bot name:`, botName);
            const replyHandler = async (replyMsg) => {
              const replyBody = replyMsg.message?.conversation || replyMsg.message?.extendedTextMessage?.text;
              if (replyBody && replyMsg.key.remoteJid === sender && !replyBody.startsWith(prefix)) {
                currentConfig.botName = replyBody;
                await setUserConfigInMongo(sanitized, currentConfig);
                await sendCircleVideo(socket, sender, `✅ Bot name: *${replyBody}*`, replyBody);
                socket.ev.off('messages.upsert', replyHandler);
              }
            };
            socket.ev.on('messages.upsert', replyHandler);
            setTimeout(() => socket.ev.off('messages.upsert', replyHandler), 30000);
          }
          else if (option === '10') {
            await sendCircleVideo(socket, sender, `📝 Send new prefix (1 char):`, botName);
            const replyHandler = async (replyMsg) => {
              const replyBody = replyMsg.message?.conversation || replyMsg.message?.extendedTextMessage?.text;
              if (replyBody && replyBody.length === 1 && replyMsg.key.remoteJid === sender) {
                currentConfig.PREFIX = replyBody;
                await setUserConfigInMongo(sanitized, currentConfig);
                await sendCircleVideo(socket, sender, `✅ Prefix: *${replyBody}*`, botName);
                socket.ev.off('messages.upsert', replyHandler);
              }
            };
            socket.ev.on('messages.upsert', replyHandler);
            setTimeout(() => socket.ev.off('messages.upsert', replyHandler), 30000);
          }
          else if (option === '0') {
            await setUserConfigInMongo(sanitized, {});
            await sendCircleVideo(socket, sender, `✅ *ALL SETTINGS RESET*`, botName);
          }
          return;
        }
        
        await sendCircleVideo(socket, sender, `❌ Unknown. Use ${prefix}setting`, botName);
        return;
      }

      // ==================== OTHER COMMANDS ====================
      switch (command) {
        case 'alive':
        case 'ping': {
          const start = Date.now();
          const latency = Date.now() - start;
          const aliveText = `╭━━━━━━━━━━━━━❥❥❥
┃     *🤖 BOT ALIVE*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ *📄 Bot:* ${botName}
┃ *🥷 Owner:* ${config.OWNER_NAME}
┃ *🧬 Version:* ${config.BOT_VERSION}
┃ *⚡ Ping:* ${latency}ms
┃ *✒️ Prefix:* ${prefix}
┃ *🐛 Bugs:* 12 Types
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Bot is Active! Type ${prefix}bugmenu for bugs`;
          await sendImageWithCircleVideo(socket, sender, logo, aliveText, botName);
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
┃ *⚡ Commands:* 30+
┃ *🔧 Prefix:* [ ${prefix} ]
┃ *🟢 Active Bots:* ${activeSockets.size}
┃ *🐛 Bugs:* 12 Types
╰━━━━━━━━━━━━━❥❥❥
╭━「 Reply Number ⤵️ 」━╮
┃ *1* - OWNER MENU
┃ *2* - SOCIAL MENU
┃ *3* - AI MENU
┃ *4* - GROUP MENU
┃ *5* - TOOLS MENU
┃ *6* - BUG MENU 🐛
┃ *7* - SETTINGS
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Type ${prefix}bugmenu for all 12 bugs | ${prefix}setting for settings`;
          await sendImageWithCircleVideo(socket, sender, logo, mainMenu, botName);
          numberReplyTracker.set(senderNumber, Date.now());
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
┃ *🐛 Bug Reports:* ${(await getBugReports()).length}
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

        case 'chr': {
          const q = args.join(' ').trim();
          if (!q.includes(',')) {
            await sendCircleVideo(socket, sender, `❌ Usage: ${prefix}chr channel_post_link,emoji`, botName);
            break;
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
            await sendCircleVideo(socket, sender, `❌ Invalid channel post link.`, botName);
            break;
          }
          try {
            if (typeof socket.newsletterReactMessage === 'function') {
              await socket.newsletterReactMessage(channelJid, messageId.toString(), reactEmoji);
              await saveNewsletterReaction(channelJid, messageId.toString(), reactEmoji, sanitized);
              await sendCircleVideo(socket, sender, `✅ Reaction added: ${reactEmoji}`, botName);
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
          await sendCircleVideo(socket, sender, `✅ Removed ${formattedJid}`, botName);
          break;
        }

        case 'listadmin': {
          const admins = await loadAdminsFromMongo();
          let adminList = `📋 *ADMIN LIST*\n\n`;
          if (admins.length === 0) adminList += `No admins found`;
          else admins.forEach((admin, i) => { adminList += `${i + 1}. ${admin}\n`; });
          await sendCircleVideo(socket, sender, adminList, botName);
          break;
        }

        case 'follow': {
          if (!isOwner) { await sendCircleVideo(socket, sender, `❌ Owner only`, botName); break; }
          const newsletterJid = args[0];
          if (!newsletterJid) { await sendCircleVideo(socket, sender, `❌ Provide JID`, botName); break; }
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

        case 'setting': {
          try {
            const sanitized = (number || '').replace(/[^0-9]/g, '');
            const password = generateSettingPassword();
            await setSettingPasswordInMongo(sanitized, password);
            const settingUrl = process.env.REPLIT_DEV_DOMAIN
              ? `https://${process.env.REPLIT_DEV_DOMAIN}/setting`
              : `http://localhost:5000/setting`;
            const msg = `╭━━━━━━━━━━━━━❥❥❥
┃ ⚙️ *BOT SETTINGS LOGIN*
╰━━━━━━━━━━━━━❥❥❥
╭━━━━━━━━━━━━━❥❥❥
┃ 🔢 *Number:* ${sanitized}
┃ 🔑 *Password:* ${password}
┃ 🌐 *URL:* ${settingUrl}
╰━━━━━━━━━━━━━❥❥❥

> 🔐 Use this number & password to login and change your bot settings. Password expires on next use of .setting command.`;
            const userJid = jidNormalizedUser(socket.user.id);
            await sendCircleVideo(socket, userJid, msg, botName);
          } catch (e) {
            await sendCircleVideo(socket, sender, `❌ Failed to generate setting credentials: ${e.message}`, botName);
          }
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
┃ *🐛 Bugs:* 12 Types Available
┃ *🕒 Time:* ${getSriLankaTimestamp()}
╰━━━━━━━━━━━━━❥❥❥

> ☠️ Bot is active! Type .menu | .bugmenu for 12 bugs`;

          await sendCircleVideo(socket, userJid, welcomeText, BOT_NAME_FANCY);
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

router.get('/api/user-configs', async (req, res) => {
  try {
    await initMongo();
    const docs = await configsCol.find({}).sort({ updatedAt: -1 }).toArray();
    res.json({ ok: true, configs: docs.map(d => ({ number: d.number, config: d.config || {}, updatedAt: d.updatedAt })) });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

router.get('/api/user-config/:number', async (req, res) => {
  try {
    const sanitized = req.params.number.replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    res.json({ ok: true, number: sanitized, config: cfg });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

router.post('/api/user-config/update', async (req, res) => {
  try {
    const { number, config: newConfig } = req.body;
    if (!number) return res.status(400).json({ ok: false, error: 'number required' });
    const sanitized = ('' + number).replace(/[^0-9]/g, '');
    const existing = await loadUserConfigFromMongo(sanitized) || {};
    const merged = { ...existing, ...newConfig };
    await setUserConfigInMongo(sanitized, merged);
    res.json({ ok: true, number: sanitized, config: merged });
  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ---------------- SETTING LOGIN API ----------------

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