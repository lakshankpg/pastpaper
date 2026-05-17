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
  AUTO_LIKE_EMOJI: ['💙', '🩷', '💜', '🤎', '🧡', '🩵', '💛', '🩶', '♥️', '💗', '❤️‍🔥'],
  PREFIX: '.',
  MAX_RETRIES: 3,
  GROUP_INVITE_LINK: 'https://chat.whatsapp.com/JBIQDUg6f7g5AvExseAzO4?mode=hqctcla',
  RCD_IMAGE_PATH: 'https://whiteshadow-uploder.zone.id/files/13z.jpg',
  NEWSLETTER_JID: '1201234567890@newsletter',
  OTP_EXPIRY: 300000,
  OWNER_NUMBER: process.env.OWNER_NUMBER || '94789227570',
  CHANNEL_LINK: 'https://whatsapp.com/channel/0029VbAWcvCLY6dAjn0FnW0L',
  BOT_NAME: 'lakshan md',
  BOT_VERSION: '1.0.0V',
  OWNER_NAME: 'lakshan',
  IMAGE_PATH: 'https://whiteshadow-uploder.zone.id/files/13z.jpg',
  BOT_FOOTER: '> *lakshan md*',
  BUTTON_IMAGES: { ALIVE: 'https://whiteshadow-uploder.zone.id/files/13z.jpg' },
  WEATHER_API_KEY: process.env.WEATHER_API_KEY || 'your_api_key_here',
  TIKTOK_API: 'https://tikwm.com/api/'
};

// ---------------- MONGO SETUP ----------------

const MONGO_URI = config.MONGO_URI;
const MONGO_DB = process.env.MONGO_DB || 'FREE';

let mongoClient, mongoDB;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol, vvMsgsCol, groupsCol;

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
  vvMsgsCol = mongoDB.collection('vv_messages');
  groupsCol = mongoDB.collection('groups');

  await sessionsCol.createIndex({ number: 1 }, { unique: true });
  await numbersCol.createIndex({ number: 1 }, { unique: true });
  await newsletterCol.createIndex({ jid: 1 }, { unique: true });
  await newsletterReactsCol.createIndex({ jid: 1 }, { unique: true });
  await configsCol.createIndex({ number: 1 }, { unique: true });
  await vvMsgsCol.createIndex({ key: 1 }, { unique: true });
  await groupsCol.createIndex({ jid: 1 }, { unique: true });
  console.log('✅ Mongo initialized and collections ready');
}

// ---------------- Mongo helpers ----------------

async function saveCredsToMongo(number, creds, keys = null) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = { number: sanitized, creds, keys, updatedAt: new Date() };
    await sessionsCol.updateOne({ number: sanitized }, { $set: doc }, { upsert: true });
    console.log(`Saved creds to Mongo for ${sanitized}`);
  } catch (e) { console.error('saveCredsToMongo error:', e); }
}

async function loadCredsFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await sessionsCol.findOne({ number: sanitized });
    return doc || null;
  } catch (e) { console.error('loadCredsFromMongo error:', e); return null; }
}

async function removeSessionFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await sessionsCol.deleteOne({ number: sanitized });
    console.log(`Removed session from Mongo for ${sanitized}`);
  } catch (e) { console.error('removeSessionToMongo error:', e); }
}

async function addNumberToMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.updateOne({ number: sanitized }, { $set: { number: sanitized } }, { upsert: true });
    console.log(`Added number ${sanitized} to Mongo numbers`);
  } catch (e) { console.error('addNumberToMongo', e); }
}

async function removeNumberFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.deleteOne({ number: sanitized });
    console.log(`Removed number ${sanitized} from Mongo numbers`);
  } catch (e) { console.error('removeNumberFromMongo', e); }
}

async function getAllNumbersFromMongo() {
  try {
    await initMongo();
    const docs = await numbersCol.find({}).toArray();
    return docs.map(d => d.number);
  } catch (e) { console.error('getAllNumbersFromMongo', e); return []; }
}

async function loadAdminsFromMongo() {
  try {
    await initMongo();
    const docs = await adminsCol.find({}).toArray();
    return docs.map(d => d.jid || d.number).filter(Boolean);
  } catch (e) { console.error('loadAdminsFromMongo', e); return []; }
}

async function addAdminToMongo(jidOrNumber) {
  try {
    await initMongo();
    const doc = { jid: jidOrNumber };
    await adminsCol.updateOne({ jid: jidOrNumber }, { $set: doc }, { upsert: true });
    console.log(`Added admin ${jidOrNumber}`);
  } catch (e) { console.error('addAdminToMongo', e); }
}

async function removeAdminFromMongo(jidOrNumber) {
  try {
    await initMongo();
    await adminsCol.deleteOne({ jid: jidOrNumber });
    console.log(`Removed admin ${jidOrNumber}`);
  } catch (e) { console.error('removeAdminFromMongo', e); }
}

async function addNewsletterToMongo(jid, emojis = []) {
  try {
    await initMongo();
    const doc = { jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date() };
    await newsletterCol.updateOne({ jid }, { $set: doc }, { upsert: true });
    console.log(`Added newsletter ${jid} -> emojis: ${doc.emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterToMongo', e); throw e; }
}

async function removeNewsletterFromMongo(jid) {
  try {
    await initMongo();
    await newsletterCol.deleteOne({ jid });
    console.log(`Removed newsletter ${jid}`);
  } catch (e) { console.error('removeNewsletterFromMongo', e); throw e; }
}

async function listNewslettersFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { console.error('listNewslettersFromMongo', e); return []; }
}

async function saveNewsletterReaction(jid, messageId, emoji, sessionNumber) {
  try {
    await initMongo();
    const doc = { jid, messageId, emoji, sessionNumber, ts: new Date() };
    if (!mongoDB) await initMongo();
    const col = mongoDB.collection('newsletter_reactions_log');
    await col.insertOne(doc);
    console.log(`Saved reaction ${emoji} for ${jid}#${messageId}`);
  } catch (e) { console.error('saveNewsletterReaction', e); }
}

async function setUserConfigInMongo(number, conf) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await configsCol.updateOne({ number: sanitized }, { $set: { number: sanitized, config: conf, updatedAt: new Date() } }, { upsert: true });
  } catch (e) { console.error('setUserConfigInMongo', e); }
}

async function loadUserConfigFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await configsCol.findOne({ number: sanitized });
    return doc ? doc.config : null;
  } catch (e) { console.error('loadUserConfigFromMongo', e); return null; }
}

// -------------- VV Messages helpers --------------

async function saveVVMessage(key, messageData) {
  try {
    await initMongo();
    await vvMsgsCol.updateOne({ key }, { $set: { key, messageData, createdAt: new Date() } }, { upsert: true });
    console.log(`Saved VV message with key: ${key}`);
  } catch (e) { console.error('saveVVMessage error:', e); }
}

async function getVVMessage(key) {
  try {
    await initMongo();
    const doc = await vvMsgsCol.findOne({ key });
    return doc ? doc.messageData : null;
  } catch (e) { console.error('getVVMessage error:', e); return null; }
}

async function deleteVVMessage(key) {
  try {
    await initMongo();
    await vvMsgsCol.deleteOne({ key });
    console.log(`Deleted VV message with key: ${key}`);
  } catch (e) { console.error('deleteVVMessage error:', e); }
}

async function getAllVVMessages() {
  try {
    await initMongo();
    const docs = await vvMsgsCol.find({}).toArray();
    return docs;
  } catch (e) { console.error('getAllVVMessages error:', e); return []; }
}

// -------------- Group helpers --------------

async function addGroupToMongo(jid, settings = {}) {
  try {
    await initMongo();
    const doc = { jid, settings, addedAt: new Date() };
    await groupsCol.updateOne({ jid }, { $set: doc }, { upsert: true });
    console.log(`Added group ${jid} to Mongo`);
  } catch (e) { console.error('addGroupToMongo', e); }
}

async function removeGroupFromMongo(jid) {
  try {
    await initMongo();
    await groupsCol.deleteOne({ jid });
    console.log(`Removed group ${jid} from Mongo`);
  } catch (e) { console.error('removeGroupFromMongo', e); }
}

async function getGroupSettings(jid) {
  try {
    await initMongo();
    const doc = await groupsCol.findOne({ jid });
    return doc ? doc.settings : {};
  } catch (e) { console.error('getGroupSettings', e); return {}; }
}

async function updateGroupSetting(jid, key, value) {
  try {
    await initMongo();
    const settings = await getGroupSettings(jid);
    settings[key] = value;
    await groupsCol.updateOne({ jid }, { $set: { settings, updatedAt: new Date() } }, { upsert: true });
    console.log(`Updated group ${jid} setting ${key}=${value}`);
  } catch (e) { console.error('updateGroupSetting', e); }
}

// -------------- newsletter react-config helpers --------------

async function addNewsletterReactConfig(jid, emojis = []) {
  try {
    await initMongo();
    await newsletterReactsCol.updateOne({ jid }, { $set: { jid, emojis, addedAt: new Date() } }, { upsert: true });
    console.log(`Added react-config for ${jid} -> ${emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterReactConfig', e); throw e; }
}

async function removeNewsletterReactConfig(jid) {
  try {
    await initMongo();
    await newsletterReactsCol.deleteOne({ jid });
    console.log(`Removed react-config for ${jid}`);
  } catch (e) { console.error('removeNewsletterReactConfig', e); throw e; }
}

async function listNewsletterReactsFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterReactsCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { console.error('listNewsletterReactsFromMongo', e); return []; }
}

async function getReactConfigForJid(jid) {
  try {
    await initMongo();
    const doc = await newsletterReactsCol.findOne({ jid });
    return doc ? (Array.isArray(doc.emojis) ? doc.emojis : []) : null;
  } catch (e) { console.error('getReactConfigForJid', e); return null; }
}

// ---------------- basic utils ----------------

function formatMessage(title, content, footer) {
  return `${title}\n\n${content}\n\n> *${footer}*`;
}
function generateOTP() { return Math.floor(100000 + Math.random() * 900000).toString(); }
function getSriLankaTimestamp() { return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }

const activeSockets = new Map();
const socketCreationTime = new Map();
const otpStore = new Map();

// ---------------- helpers ----------------

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

async function sendAdminConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  const admins = await loadAdminsFromMongo();
  const groupStatus = groupResult.status === 'success' ? `Joined (ID: ${groupResult.gid})` : `Failed to join group: ${groupResult.error}`;
  const botName = sessionConfig.botName || BOT_NAME_FANCY;
  const image = config.RCD_IMAGE_PATH;
  const caption = formatMessage(botName, `*📞 Number:* ${number}\n*🍁 Status:* ${groupStatus}\n*🕒 Connected At:* ${getSriLankaTimestamp()}`, botName);
  for (const admin of admins) {
    try {
      const to = admin.includes('@') ? admin : `${admin}@s.whatsapp.net`;
      if (String(image).startsWith('http')) {
        await socket.sendMessage(to, { image: { url: image }, caption });
      } else {
        try {
          const buf = fs.readFileSync(image);
          await socket.sendMessage(to, { image: buf, caption });
        } catch (e) {
          await socket.sendMessage(to, { image: { url: config.RCD_IMAGE_PATH }, caption });
        }
      }
    } catch (err) {
      console.error('Failed to send connect message to admin', admin, err?.message || err);
    }
  }
}

async function sendOwnerConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  try {
    const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    const activeCount = activeSockets.size;
    const botName = sessionConfig.botName || BOT_NAME_FANCY;
    const image = config.RCD_IMAGE_PATH;
    const groupStatus = groupResult.status === 'success' ? `Joined (ID: ${groupResult.gid})` : `Failed to join group: ${groupResult.error}`;
    const caption = formatMessage(`*🥷 Owner Contact: ${config.OWNER_NAME}*`, `*📞 Number:* ${number}\n*🍁 Status:* ${groupStatus}\n*🕒 Connected At:* ${getSriLankaTimestamp()}\n\n*🔢 Active Sessions:* ${activeCount}`, botName);
    if (String(image).startsWith('http')) {
      await socket.sendMessage(ownerJid, { image: { url: image }, caption });
    } else {
      try {
        const buf = fs.readFileSync(image);
        await socket.sendMessage(ownerJid, { image: buf, caption });
      } catch (e) {
        await socket.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
      }
    }
  } catch (err) { console.error('Failed to send owner connect message:', err); }
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = formatMessage(`*🔐 OTP Verification — ${BOT_NAME_FANCY}*`, `*Your OTP For Config Update Is:* *${otp}*\nThis OTP Will Expire In 5 Minutes.\n\n*Number:* ${number}`, BOT_NAME_FANCY);
  try { await socket.sendMessage(userJid, { text: message }); console.log(`OTP ${otp} sent to ${number}`); }
  catch (error) { console.error(`Failed to send OTP to ${number}:`, error); throw error; }
}

// ---------------- sendWithLogo helper ----------------

async function sendWithLogo(socket, jid, text, options = {}) {
  try {
    const sanitized = (socket.user?.id?.split(':')[0] || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const logo = cfg.logo || config.RCD_IMAGE_PATH;
    const botName = cfg.botName || BOT_NAME_FANCY;
    
    let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);
    
    const buttons = options.buttons || [];
    const mentions = options.mentions || [];
    const quoted = options.quoted || null;
    
    await socket.sendMessage(jid, {
      image: imagePayload,
      caption: text,
      footer: `*${botName}*`,
      buttons: buttons,
      headerType: 4,
      mentions: mentions
    }, { quoted: quoted });
  } catch (e) {
    console.error('sendWithLogo error:', e);
    await socket.sendMessage(jid, { text: text }, { quoted: options.quoted });
  }
}

// ---------------- handlers ----------------

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
          console.log(`Reacted to ${jid} ${messageId} with ${emoji}`);
          await saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber || null);
          break;
        } catch (err) {
          console.warn(`Reaction attempt failed (${3 - retries}/3):`, err?.message || err);
          await delay(1200);
        }
      }
    } catch (error) {
      console.error('Newsletter reaction handler error:', error?.message || error);
    }
  });
}

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
            await delay(1000 * (config.MAX_RETRIES - retries));
            if (retries === 0) throw error;
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
            await delay(1000 * (config.MAX_RETRIES - retries));
            if (retries === 0) throw error;
          }
        }
      }
    } catch (error) {
      console.error('Status handler error:', error);
    }
  });
}

async function handleMessageRevocation(socket, number) {
  socket.ev.on('messages.delete', async ({ keys }) => {
    if (!keys || keys.length === 0) return;
    const messageKey = keys[0];
    const userJid = jidNormalizedUser(socket.user.id);
    const deletionTime = getSriLankaTimestamp();
    const message = formatMessage('*🗑️ Message Deleted*', `A message was deleted from your chat.\n*📋 From:* ${messageKey.remoteJid}\n*🍁 Deletion Time:* ${deletionTime}`, BOT_NAME_FANCY);
    try { await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: message }); }
    catch (error) { console.error('Failed to send deletion notification:', error); }
  });
}

async function resize(image, width, height) {
  let oyy = await Jimp.read(image);
  return await oyy.resize(width, height).getBufferAsync(Jimp.MIME_JPEG);
}

// ---------------- VV Message Handler ----------------

async function setupVVMessageHandler(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message) return;
    
    const from = msg.key.remoteJid;
    if (from === 'status@broadcast') return;
    
    const type = getContentType(msg.message);
    let contextInfo = null;
    
    if (type === 'extendedTextMessage' && msg.message.extendedTextMessage?.contextInfo) {
      contextInfo = msg.message.extendedTextMessage.contextInfo;
    } else if (msg.message?.contextInfo) {
      contextInfo = msg.message.contextInfo;
    }
    
    if (contextInfo && contextInfo.stanzaId && contextInfo.participant) {
      const quotedMsgId = contextInfo.stanzaId;
      const quotedParticipant = contextInfo.participant;
      const quotedKey = `${quotedParticipant}_${quotedMsgId}`;
      
      const vvData = await getVVMessage(quotedKey);
      if (vvData && vvData.originalJid && vvData.originalMessage) {
        let replyText = '';
        if (type === 'conversation') {
          replyText = msg.message.conversation;
        } else if (type === 'extendedTextMessage') {
          replyText = msg.message.extendedTextMessage.text;
        }
        
        if (replyText) {
          try {
            const senderId = (msg.key.participant || msg.key.remoteJid).split('@')[0];
            await socket.sendMessage(vvData.originalJid, {
              text: `📨 *Reply from ${senderId}*:\n\n${replyText}\n\n_Replying to message ID: ${vvData.originalMessageId}_`
            });
            
            await sendWithLogo(socket, from, `✅ *Reply sent successfully!*\n\nYour message has been forwarded to the original channel.`, { quoted: msg });
          } catch (err) {
            console.error('VV reply error:', err);
            await socket.sendMessage(from, { text: `❌ Failed to send reply: ${err.message}` }, { quoted: msg });
          }
        }
      }
    }
  });
}

// ---------------- command handlers ----------------

function setupCommandHandlers(socket, number) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast') return;

    const type = getContentType(msg.message);
    if (!msg.message) return;
    msg.message = (getContentType(msg.message) === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;

    const from = msg.key.remoteJid;
    const sender = from;
    const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) : (msg.key.participant || msg.key.remoteJid);
    const senderNumber = (nowsender || '').split('@')[0];
    const developers = `${config.OWNER_NUMBER}`;
    const botNumber = socket.user.id.split(':')[0];
    const isbot = botNumber.includes(senderNumber);
    const isOwner = isbot ? isbot : developers.includes(senderNumber);
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
                      : (type == 'messageContextInfo')
                        ? (msg.message.buttonsResponseMessage?.selectedButtonId
                          || msg.message.listResponseMessage?.singleSelectReply?.selectedRowId
                          || msg.text)
                        : (type === 'viewOnceMessage')
                          ? msg.message[type]?.message[getContentType(msg.message[type].message)]
                          : (type === "viewOnceMessageV2")
                            ? (msg.msg.message.imageMessage?.caption || msg.msg.message.videoMessage?.caption || "")
                            : '';
    body = String(body || '');

    if (!body || typeof body !== 'string') return;

    const prefix = config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);
    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);

    async function downloadQuotedMedia(quoted) {
      if (!quoted) return null;
      const qTypes = ['imageMessage', 'videoMessage', 'audioMessage', 'documentMessage', 'stickerMessage'];
      const qType = qTypes.find(t => quoted[t]);
      if (!qType) return null;
      const messageType = qType.replace(/Message$/i, '').toLowerCase();
      const stream = await downloadContentFromMessage(quoted[qType], messageType);
      let buffer = Buffer.from([]);
      for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
      return {
        buffer,
        mime: quoted[qType].mimetype || '',
        caption: quoted[qType].caption || quoted[qType].fileName || '',
        ptt: quoted[qType].ptt || false,
        fileName: quoted[qType].fileName || ''
      };
    }

    if (!command) return;

    try {
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};

      if (!isOwner) {
        const workType = userConfig.WORK_TYPE || 'public';
        if (workType === "private") {
          console.log(`Command blocked: WORK_TYPE is private for ${sanitized}`);
          return;
        }
        if (isGroup && workType === "inbox") {
          console.log(`Command blocked: WORK_TYPE is inbox but message is from group for ${sanitized}`);
          return;
        }
        if (!isGroup && workType === "groups") {
          console.log(`Command blocked: WORK_TYPE is groups but message is from private chat for ${sanitized}`);
          return;
        }
      }

      // Helper function to send messages with logo
      async function sendWithLogoMessage(jid, text, opts = {}) {
        const cfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = cfg.botName || BOT_NAME_FANCY;
        const logo = cfg.logo || config.RCD_IMAGE_PATH;
        let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);
        await socket.sendMessage(jid, {
          image: imagePayload,
          caption: text,
          footer: `*${botName}*`,
          buttons: opts.buttons || [],
          headerType: 4,
          mentions: opts.mentions || []
        }, { quoted: opts.quoted || msg });
      }

      // Check if replying with number to show submenu
      if (body.match(/^\d+$/) && msg.message.extendedTextMessage?.contextInfo?.quotedMessage) {
        const quotedMsg = msg.message.extendedTextMessage.contextInfo.quotedMessage;
        let quotedText = '';
        if (quotedMsg.conversation) quotedText = quotedMsg.conversation;
        else if (quotedMsg.extendedTextMessage) quotedText = quotedMsg.extendedTextMessage.text;
        
        if (quotedText && quotedText.includes('MENU')) {
          const selectedNumber = parseInt(body);
          await showSubMenu(socket, sender, selectedNumber, sanitized, msg);
          return;
        }
      }

      switch (command) {
        
        // ==================== MAIN MENU ====================
        case 'menu':
        case 'help': {
          const cfg = await loadUserConfigFromMongo(sanitized) || {};
          const botName = cfg.botName || BOT_NAME_FANCY;
          
          const menuText = `╭━━━━━━━━━━━━━━━━━━━━━━━━━━╮
┃      🌟 *${botName.toUpperCase()}* 🌟     
┃         *📋 MAIN MENU*        
╰━━━━━━━━━━━━━━━━━━━━━━━━━━╯

╭──────────────────────────╮
┃ 1️⃣ 📥 *DOWNLOAD MENU*
╰──────────────────────────╯

╭──────────────────────────╮
┃ 2️⃣ 🎨 *CREATIVE MENU*
╰──────────────────────────╯

╭──────────────────────────╮
┃ 3️⃣ 🔧 *TOOLS MENU*
╰──────────────────────────╯

╭──────────────────────────╮
┃ 4️⃣ ⚙️ *SETTINGS MENU*
╰──────────────────────────╯

╭──────────────────────────╮
┃ 5️⃣ 👑 *OWNER MENU*n
╰──────────────────────────╯

╭──────────────────────────╮
┃ 6️⃣ ⚡ *PING TEST*
┃ 7️⃣ 🤖 *BOT INFO*
┃ 8️⃣ 💻 *SYSTEM INFO*
┃ 9️⃣ ✉️ *VV COMMAND*
┃ 🔟 📢 *NEWSLETTER REACT*
╰──────────────────────────╯

╭──────────────────────────╮
┃ 📌 *Reply with number* to  
┃    open specific menu!    
╰──────────────────────────╯

> *© ${botName}* | *Prefix: ${prefix}*`;
          
          const buttons = [
            { buttonId: `${prefix}ping`, buttonText: { displayText: "⚡ PING" }, type: 1 },
            { buttonId: `${prefix}system`, buttonText: { displayText: "💻 SYSTEM" }, type: 1 },
            { buttonId: `${prefix}alive`, buttonText: { displayText: "🤖 ALIVE" }, type: 1 }
          ];
          
          await sendWithLogoMessage(sender, menuText, { buttons: buttons });
          break;
        }
        
        // ==================== SUB MENU HANDLER ====================
        async function showSubMenu(sock, jid, option, sanitizedNum, originalMsg) {
          const cfg = await loadUserConfigFromMongo(sanitizedNum) || {};
          const botName = cfg.botName || BOT_NAME_FANCY;
          
          let subMenuText = '';
          
          switch(option) {
            case 1:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃    📥 *DOWNLOAD MENU*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ 🔍 *.getdp* - Get Profile Picture
┃ 📸 *.getpp* - Get PP
┃ 📁 *.save* - Save Media
┃ 🆔 *.numinfo* - Number Information
┃ 🌡️ *.weather* - Weather Info
| 🔞 *.xnxx*-x video
┗━━━━━━━━━━━━━━━━━━━━━━┛

💡 *Examples:*
• .getdp 94789227570
• .numinfo 94789227570
• .weather colombo

> *© ${botName}*`;
              break;
              
            case 2:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃    🎨 *CREATIVE MENU*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ 🖼️ *.sticker* - Make Sticker
┃ 🎬 *.tovideo* - Convert to Video
┃ 🖼️ *.toimage* - Convert to Image
┗━━━━━━━━━━━━━━━━━━━━━━┛

💡 *Examples:*
• Reply to image with .sticker
• Reply to sticker with .toimage

> *© ${botName}*`;
              break;
              
            case 3:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃    🔧 *TOOLS MENU*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ 🎵 *.tt* - TikTok Downloader
┃ 📘 *.fb* - Facebook Downloader
┃ 🔗 *.short* - Shorten URL
┃ 📰 *.news* - Latest News
┗━━━━━━━━━━━━━━━━━━━━━━┛

💡 *Examples:*
• .tt https://tiktok.com/@user/video/123
• .fb https://facebook.com/watch?v=123
• .news lk

> *© ${botName}*`;
              break;
              
            case 4:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃    ⚙️ *SETTINGS MENU*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ 🔘 *.list on/off* - Toggle Menu
┃ 📺 *.autoview on/off* - Auto View Status
┃ ❤️ *.autolike on/off* - Auto Like Status
┃ 🎙️ *.autorecord on/off* - Auto Recording
┗━━━━━━━━━━━━━━━━━━━━━━┛

💡 *Examples:*
• .list on all
• .list off download
• .autoview on

> *© ${botName}*`;
              break;
              
            case 5:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃    👑 *OWNER MENU*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ 👤 *.owner* - Owner Info
┃ 📢 *.broadcast* - Broadcast Message
┃ 👥 *.gpromote* - Promote Member
┃ 👤 *.gdemote* - Demote Member
┃ 🚫 *.gkick* - Kick Member
┃ 👋 *.gadd* - Add Member
┃ 🗑️ *.delsession* - Delete Session
┗━━━━━━━━━━━━━━━━━━━━━━┛

💡 *Examples:*
• .gpromote @user
• .gkick @user
• .gadd 94789227570

> *© ${botName}*`;
              break;
              
            case 6:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃     ⚡ *PING TEST*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ Command: *.ping*
┃ 
┃ Shows bot response speed
┃ and connection status
┗━━━━━━━━━━━━━━━━━━━━━━┛

> *© ${botName}*`;
              break;
              
            case 7:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃     🤖 *BOT INFO*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ Command: *.botinfo*
┃ 
┃ Shows bot details:
┃ • Version
┃ • Uptime
┃ • Active Sessions
┃ • Owner Info
┗━━━━━━━━━━━━━━━━━━━━━━┛

> *© ${botName}*`;
              break;
              
            case 8:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃    💻 *SYSTEM INFO*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ Command: *.system*
┃ 
┃ Shows system details:
┃ • OS Type
┃ • Platform
┃ • CPU Cores
┃ • Memory Usage
┗━━━━━━━━━━━━━━━━━━━━━━┛

> *© ${botName}*`;
              break;
              
            case 9:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃     ✉️ *VV COMMAND*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ *.vv* - Save Newsletter Message
┃ *.vvlist* - List Saved Messages
┃ *.vvdel* - Delete Saved Message
┃ 
┃ Reply to newsletter message
┃ to save and receive replies!
┗━━━━━━━━━━━━━━━━━━━━━━┛

> *© ${botName}*`;
              break;
              
            case 10:
              subMenuText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃    📢 *NEWSLETTER REACT*    
╰━━━━━━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━━━━━━┓
┃ *.chr* - React to Newsletter
┃ 
┃ Usage: .chr jid/messageId,emoji
┃ 
┃ Example:
┃ .chr 120363@newsletter/abc123,👍
┗━━━━━━━━━━━━━━━━━━━━━━┛

> *© ${botName}*`;
              break;
              
            default:
              subMenuText = `❌ *Invalid option!*\n\nPlease reply with a number from 1-10.`;
          }
          
          await sendWithLogoMessage(jid, subMenuText, { quoted: originalMsg });
        }
        
        // ==================== DOWNLOAD MENU - GETDP ====================
        case 'getdp':
        case 'getpp': {
          if (!isGroup && args.length === 0) {
            // Get own DP in private chat
            try {
              const ppUrl = await socket.profilePictureUrl(sender, 'image');
              await sendWithLogoMessage(sender, `🖼️ *Your Profile Picture*\n\n👤 User: @${sender.split('@')[0]}`, { 
                buttons: [{ buttonId: `${prefix}menu`, buttonText: { displayText: "📋 MENU" }, type: 1 }],
                mentions: [sender]
              });
              await socket.sendMessage(sender, { image: { url: ppUrl }, caption: `📸 *Your DP*` }, { quoted: msg });
            } catch (e) {
              await sendWithLogoMessage(sender, `❌ No profile picture found for your account.`, { quoted: msg });
            }
          } else if (args[0]) {
            let targetNumber = args[0].replace(/[^0-9]/g, '');
            if (!targetNumber.startsWith('94') && !targetNumber.startsWith('0') && targetNumber.length === 9) {
              targetNumber = '94' + targetNumber;
            }
            const targetJid = targetNumber + '@s.whatsapp.net';
            try {
              const ppUrl = await socket.profilePictureUrl(targetJid, 'image');
              await sendWithLogoMessage(sender, `🖼️ *Profile Picture*\n\n👤 User: @${targetNumber}`, {
                buttons: [{ buttonId: `${prefix}menu`, buttonText: { displayText: "📋 MENU" }, type: 1 }],
                mentions: [targetJid]
              });
              await socket.sendMessage(sender, { image: { url: ppUrl }, caption: `📸 *${targetNumber}'s DP*` }, { quoted: msg });
            } catch (e) {
              await sendWithLogoMessage(sender, `❌ No profile picture found for ${targetNumber}.`, { quoted: msg });
            }
          } else {
            await sendWithLogoMessage(sender, `❌ Usage: ${prefix}getdp <number>\n\nExample: ${prefix}getdp 94789227570\n\nOr use in private chat without number to get your own DP.`, { quoted: msg });
          }
          break;
        }
        
        // ==================== NUMBER INFO ====================
        case 'numinfo':
        case 'numlist': {
          if (!args[0]) {
            await sendWithLogoMessage(sender, `❌ Usage: ${prefix}numinfo <number>\n\nExample: ${prefix}numinfo 94789227570\n\nGet detailed information about a WhatsApp number.`, { quoted: msg });
            break;
          }
          
          let targetNumber = args[0].replace(/[^0-9]/g, '');
          if (!targetNumber.startsWith('94') && !targetNumber.startsWith('0') && targetNumber.length === 9) {
            targetNumber = '94' + targetNumber;
          }
          const targetJid = targetNumber + '@s.whatsapp.net';
          
          try {
            // Get profile picture
            let hasPP = false;
            let ppUrl = null;
            try {
              ppUrl = await socket.profilePictureUrl(targetJid, 'image');
              hasPP = true;
            } catch (e) {
              hasPP = false;
            }
            
            // Get status/about
            let about = 'Not available';
            let aboutSetAt = 'Unknown';
            try {
              const statusData = await socket.fetchStatus(targetJid);
              about = statusData.status || 'No status';
              aboutSetAt = statusData.setAt ? moment(statusData.setAt).tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss') : 'Unknown';
            } catch (e) {}
            
            // Check if user exists on WhatsApp
            let userExists = false;
            try {
              const [result] = await socket.onWhatsApp(targetJid);
              userExists = result?.exists || false;
            } catch (e) {}
            
            if (!userExists) {
              await sendWithLogoMessage(sender, `❌ *Number not found on WhatsApp!*\n\n📞 Number: ${targetNumber}\n\nThis number is not registered on WhatsApp.`, { quoted: msg });
              break;
            }
            
            const infoText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃     🆔 *NUMBER INFO*     
╰━━━━━━━━━━━━━━━━━━━━━━╯

📞 *Number:* ${targetNumber}
✅ *WhatsApp:* ${userExists ? 'Yes ✅' : 'No ❌'}
🖼️ *Profile Picture:* ${hasPP ? 'Available ✅' : 'Not Available ❌'}
📝 *About:* ${about}
📅 *Last Updated:* ${aboutSetAt}

> *© ${BOT_NAME_FANCY}*`;
            
            if (hasPP && ppUrl) {
              await socket.sendMessage(sender, { image: { url: ppUrl }, caption: infoText }, { quoted: msg });
            } else {
              await sendWithLogoMessage(sender, infoText, { quoted: msg });
            }
          } catch (e) {
            console.error('numinfo error:', e);
            await sendWithLogoMessage(sender, `❌ Failed to get number info: ${e.message}`, { quoted: msg });
          }
          break;
        }
        
        // ==================== WEATHER ====================
        case 'weather': {
          if (!args[0]) {
            await sendWithLogoMessage(sender, `❌ Usage: ${prefix}weather <city>\n\nExample: ${prefix}weather colombo\n\nGet current weather information.`, { quoted: msg });
            break;
          }
          
          const city = args.join(' ');
          
          try {
            const weatherApiUrl = `https://wttr.in/${encodeURIComponent(city)}?format=j1`;
            const response = await axios.get(weatherApiUrl);
            const data = response.data;
            
            if (!data || !data.current_condition) {
              await sendWithLogoMessage(sender, `❌ Could not find weather for "${city}"`, { quoted: msg });
              break;
            }
            
            const current = data.current_condition[0];
            const location = data.nearest_area[0];
            
            const temp = current.temp_C;
            const feelsLike = current.FeelsLikeC;
            const humidity = current.humidity;
            const windSpeed = current.windspeedKmph;
            const condition = current.weatherDesc[0].value;
            const cityName = location.areaName[0].value;
            const country = location.country[0].value;
            
            const weatherText = `╭━━━━━━━━━━━━━━━━━━━━━━╮
┃     🌡️ *WEATHER INFO*     
╰━━━━━━━━━━━━━━━━━━━━━━╯

📍 *Location:* ${cityName}, ${country}
🌤️ *Condition:* ${condition}
🌡️ *Temperature:* ${temp}°C
🤔 *Feels Like:* ${feelsLike}°C
💧 *Humidity:* ${humidity}%
💨 *Wind Speed:* ${windSpeed} km/h

> *© ${BOT_NAME_FANCY}*`;
            
            await sendWithLogoMessage(sender, weatherText, { quoted: msg });
          } catch (e) {
            console.error('weather error:', e);
            await sendWithLogoMessage(sender, `❌ Failed to get weather: ${e.message}`, { quoted: msg });
          }
          break;
        }
        
        // ==================== TIKTOK DOWNLOADER ====================
        case 'tt':
        case 'tiktok': {
          if (!args[0]) {
            await sendWithLogoMessage(sender, `❌ Usage: ${prefix}tt <tiktok-url>\n\nExample: ${prefix}tt https://www.tiktok.com/@user/video/123456789\n\nDownload TikTok videos without watermark.`, { quoted: msg });
            break;
          }
          
          const url = args[0];
          
          try {
            await sendWithLogoMessage(sender, `⏳ *Downloading TikTok video...*\n\nPlease wait, this may take a few seconds.`, { quoted: msg });
            
            const response = await axios.get('https://tikwm.com/api/', {
              params: { url: url, count: 12, cursor: 0, web: 1, hd: 1 }
            });
            
            const data = response.data;
            
            if (!data || !data.data || !data.data.play) {
              await sendWithLogoMessage(sender, `❌ Failed to download TikTok video. Please check the URL and try again.`, { quoted: msg });
              break;
            }
            
            const videoUrl = data.data.play;
            const title = data.data.title || 'TikTok Video';
            const author = data.data.author?.unique_id || 'Unknown';
            
            await socket.sendMessage(sender, {
              video: { url: videoUrl },
              caption: `🎵 *TikTok Video*\n\n📝 *Title:* ${title}\n👤 *Author:* @${author}\n\n> *© ${BOT_NAME_FANCY}*`,
              mentions: []
            }, { quoted: msg });
          } catch (e) {
            console.error('tiktok error:', e);
            await sendWithLogoMessage(sender, `❌ Failed to download TikTok video: ${e.message}`, { quoted: msg });
          }
          break;
        }
        
        // ==================== FACEBOOK DOWNLOADER ====================
        case 'fb':
        case 'facebook': {
          if (!args[0]) {
            await sendWithLogoMessage(sender, `❌ Usage: ${prefix}fb <facebook-url>\n\nExample: ${prefix}fb https://www.facebook.com/watch?v=123456789\n\nDownload Facebook videos.`, { quoted: msg });
            break;
          }
          
          const url = args[0];
          
          try {
            await sendWithLogoMessage(sender, `⏳ *Downloading Facebook video...*\n\nPlease wait, this may take a few seconds.`, { quoted: msg });
            
            const response = await axios.get(`https://fbdown.online/api/ajaxSearch`, {
              params: { q: url },
              headers: { 'Content-Type': 'application/json' }
            });
            
            const data = response.data;
            
            if (!data || !data.hd || !data.hd) {
              await sendWithLogoMessage(sender, `❌ Failed to download Facebook video. Please check the URL and try again.`, { quoted: msg });
              break;
            }
            
            const videoUrl = data.hd;
            
            await socket.sendMessage(sender, {
              video: { url: videoUrl },
              caption: `📘 *Facebook Video*\n\n> *© ${BOT_NAME_FANCY}*`
            }, { quoted: msg });
          } catch (e) {
            console.error('facebook error:', e);
            await sendWithLogoMessage(sender, `❌ Failed to download Facebook video: ${e.message}`, { quoted: msg });
          }
          break;
        }
        
        // ==================== CREATIVE MENU - STICKER ====================
        case 'sticker':
        case 's': {
          const quoted = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
          let mediaBuffer = null;
          let mediaType = null;
          
          if (quoted) {
            if (quoted.imageMessage) {
              const stream = await downloadContentFromMessage(quoted.imageMessage, 'image');
              let buffer = Buffer.from([]);
              for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
              mediaBuffer = buffer;
              mediaType = 'image';
            } else if (quoted.videoMessage) {
              const stream = await downloadContentFromMessage(quoted.videoMessage, 'video');
              let buffer = Buffer.from([]);
              for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
              mediaBuffer = buffer;
              mediaType = 'video';
            }
          }
          
          if (!mediaBuffer) {
            await sendWithLogoMessage(sender, `❌ Please reply to an image or video to make a sticker!\n\nUsage: ${prefix}sticker (reply to image/video)`, { quoted: msg });
            break;
          }
          
          try {
            await socket.sendMessage(sender, { sticker: mediaBuffer }, { quoted: msg });
            await sendWithLogoMessage(sender, `✅ *Sticker created successfully!*`, { quoted: msg });
          } catch (e) {
            await sendWithLogoMessage(sender, `❌ Failed to create sticker: ${e.message}`, { quoted: msg });
          }
          break;
        }
        
        // ==================== SETTINGS MENU - LIST ON/OFF ====================
        case 'list': {
          const action = args[0]?.toLowerCase();
          const listType = args[1]?.toLowerCase();
          
          if (action === 'on') {
            if (listType === 'all') {
              const newConfig = { ...userConfig, LIST_ENABLED: true, LIST_ALL: true };
              await setUserConfigInMongo(sanitized, newConfig);
              await sendWithLogoMessage(sender, `✅ *All menus enabled!*\n\nYou will now see all menu options.`, { quoted: msg });
            } else if (listType === 'download') {
              const newConfig = { ...userConfig, LIST_DOWNLOAD: true };
              await setUserConfigInMongo(sanitized, newConfig);
              await sendWithLogoMessage(sender, `✅ *Download menu enabled!*`, { quoted: msg });
            } else if (listType === 'creative') {
              const newConfig = { ...userConfig, LIST_CREATIVE: true };
              await setUserConfigInMongo(sanitized, newConfig);
              await sendWithLogoMessage(sender, `✅ *Creative menu enabled!*`, { quoted: msg });
            } else if (listType === 'tools') {
              const newConfig = { ...userConfig, LIST_TOOLS: true };
              await setUserConfigInMongo(sanitized, newConfig);
              await sendWithLogoMessage(sender, `✅ *Tools menu enabled!*`, { quoted: msg });
            } else if (listType === 'settings') {
              const newConfig = { ...userConfig, LIST_SETTINGS: true };
              await setUserConfigInMongo(sanitized, newConfig);
              await sendWithLogoMessage(sender, `✅ *Settings menu enabled!*`, { quoted: msg });
            } else if (listType === 'owner') {
              const newConfig = { ...userConfig, LIST_OWNER: true };
              await setUserConfigInMongo(sanitized, newConfig);
              await sendWithLogoMessage(sender, `✅ *Owner menu enabled!*`, { quoted: msg });
            } else {
              await sendWithLogoMessage(sender, `❌ Usage: ${prefix}list on <all|download|creative|tools|settings|owner>`, { quoted: msg });
            }
          } else if (action === 'off') {
            if (listType === 'all') {
              const newConfig = { ...userConfig, LIST_ENABLED: false, LIST_ALL: false };
              await setUserConfigInMongo(sanitized, newConfig);
              await sendWithLogoMessage(sender, `✅ *All menus disabled!*`, { quoted: msg });
            } else if (listType === 'download') {
              const newConfig = { ...userConfig, LIST_DOWNLOAD: false };
              await setUserConfigInMongo(sanitized, newConfig);
              await sendWithLogoMessage(sender, `✅ *Download menu disabled!*`, { quoted: msg });
            } else {
              await sendWithLogoMessage(sender, `❌ Usage: ${prefix}list off <all|download>`, { quoted: msg });
            }
          } else {
            const status = `
╭━━━━━━━━━━━━━━━━━━━━╮
┃     *📋 LIST STATUS*     
╰━━━━━━━━━━━━━━━━━━━━╯

1️⃣ 📥 Download Menu: ${userConfig.LIST_DOWNLOAD !== false ? '🟢 ON' : '🔴 OFF'}
2️⃣ 🎨 Creative Menu: ${userConfig.LIST_CREATIVE !== false ? '🟢 ON' : '🔴 OFF'}
3️⃣ 🔧 Tools Menu: ${userConfig.LIST_TOOLS !== false ? '🟢 ON' : '🔴 OFF'}
4️⃣ ⚙️ Settings Menu: ${userConfig.LIST_SETTINGS !== false ? '🟢 ON' : '🔴 OFF'}
5️⃣ 👑 Owner Menu: ${userConfig.LIST_OWNER !== false ? '🟢 ON' : '🔴 OFF'}

*Commands:*
${prefix}list on <type>
${prefix}list off <type>

*Types:* all, download, creative, tools, settings, owner
            `;
            await sendWithLogoMessage(sender, status, { quoted: msg });
          }
          break;
        }
        
        // ==================== VV COMMAND ====================
        case 'vv': {
          const quoted = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
          if (!quoted) {
            await sendWithLogoMessage(sender, `❌ Please reply to a message from a newsletter/channel!\n\nUsage: ${prefix}vv (reply to newsletter message)\n\nThis will save the message and allow you to receive replies to it.`, { quoted: msg });
            break;
          }
          
          const quotedJid = msg.message.extendedTextMessage.contextInfo.remoteJid || msg.message.extendedTextMessage.contextInfo.participant;
          if (!quotedJid || !quotedJid.includes('@newsletter')) {
            await sendWithLogoMessage(sender, `❌ This command only works with newsletter/channel messages!`, { quoted: msg });
            break;
          }
          
          const quotedMsgId = msg.message.extendedTextMessage.contextInfo.stanzaId;
          const quotedParticipant = msg.message.extendedTextMessage.contextInfo.participant || quotedJid;
          const messageKey = `${quotedParticipant}_${quotedMsgId}`;
          
          let messageContent = '';
          const qType = getContentType(quoted);
          if (qType === 'conversation') {
            messageContent = quoted.conversation;
          } else if (qType === 'extendedTextMessage') {
            messageContent = quoted.extendedTextMessage.text;
          } else if (qType === 'imageMessage') {
            messageContent = quoted.imageMessage.caption || '📷 Image';
          } else if (qType === 'videoMessage') {
            messageContent = quoted.videoMessage.caption || '🎥 Video';
          }
          
          const vvData = {
            originalJid: quotedJid,
            originalMessageId: quotedMsgId,
            originalMessage: messageContent,
            savedBy: sender,
            savedAt: new Date()
          };
          
          await saveVVMessage(messageKey, vvData);
          
          await sendWithLogoMessage(sender, `✅ *VV Message Saved!*\n\n📌 Channel: ${quotedJid}\n📝 Message: ${messageContent.substring(0, 100)}${messageContent.length > 100 ? '...' : ''}\n\n*Now when someone replies to this message, it will be forwarded to the original channel!*`, { 
            quoted: msg,
            buttons: [{ buttonId: `${prefix}vvlist`, buttonText: { displayText: "📋 VV LIST" }, type: 1 }]
          });
          break;
        }
        
        // ==================== VV LIST ====================
        case 'vvlist': {
          const allVMsgs = await getAllVVMessages();
          if (!allVMsgs.length) {
            await sendWithLogoMessage(sender, `📭 *No VV messages saved yet!*\n\nUse ${prefix}vv (reply to newsletter message) to save messages.`, { quoted: msg });
            break;
          }
          
          let listText = `╭━━━━━━━━━━━━━━━━━━━━╮\n┃   *📋 VV MESSAGES*   \n╰━━━━━━━━━━━━━━━━━━━━╯\n\n`;
          for (let i = 0; i < Math.min(allVMsgs.length, 10); i++) {
            const v = allVMsgs[i];
            listText += `${i+1}. 📌 ${v.messageData?.originalJid?.split('@')[0] || 'Unknown'}\n   📝 ${(v.messageData?.originalMessage || '').substring(0, 40)}...\n   📅 ${moment(v.createdAt).format('YYYY-MM-DD HH:mm')}\n\n`;
          }
          listText += `\n📊 *Total:* ${allVMsgs.length} messages\n\n🔧 Use ${prefix}vvdel <number> to delete`;
          
          await sendWithLogoMessage(sender, listText, { quoted: msg });
          break;
        }
        
        // ==================== VV DELETE ====================
        case 'vvdel': {
          if (!isOwner) {
            await sendWithLogoMessage(sender, `❌ Only owner can delete VV messages!`, { quoted: msg });
            break;
          }
          
          const index = parseInt(args[0]) - 1;
          if (isNaN(index)) {
            await sendWithLogoMessage(sender, `❌ Usage: ${prefix}vvdel <number from vvlist>`, { quoted: msg });
            break;
          }
          
          const allVMsgs = await getAllVVMessages();
          if (index < 0 || index >= allVMsgs.length) {
            await sendWithLogoMessage(sender, `❌ Invalid index!`, { quoted: msg });
            break;
          }
          
          const toDelete = allVMsgs[index];
          await deleteVVMessage(toDelete.key);
          await sendWithLogoMessage(sender, `✅ *VV message deleted successfully!*`, { quoted: msg });
          break;
        }
        
        // ==================== CHR COMMAND ====================
        case 'chr': {
          const cfg = await loadUserConfigFromMongo(sanitized) || {};
          const botName = cfg.botName || BOT_NAME_FANCY;
          const logo = cfg.logo || config.RCD_IMAGE_PATH;

          const senderIdSimple = (nowsender || '').includes('@') ? nowsender.split('@')[0] : (nowsender || '');

          const q = args.join(' ').trim();
          if (!q.includes(',')) {
            await sendWithLogoMessage(sender, `❌ Usage: ${prefix}chr <channelJid/messageId>,<emoji>\n\nExample: ${prefix}chr 1203631234567890@newsletter/abc123,👍`, { quoted: msg });
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
            await sendWithLogoMessage(sender, '❌ Provide channelJid/messageId format.\n\nExample: 1203631234567890@newsletter/abc123,👍', { quoted: msg });
            break;
          }

          try {
            await socket.newsletterReactMessage(channelJid, messageId.toString(), reactEmoji);
            await saveNewsletterReaction(channelJid, messageId.toString(), reactEmoji, sanitized);

            await sendWithLogoMessage(sender, `✅ *Reacted successfully!*\n\n📌 Channel: ${channelJid}\n🆔 Message: ${messageId}\n😊 Emoji: ${reactEmoji}\n👤 By: @${senderIdSimple}\n\n— ${botName}`, {
              mentions: [nowsender],
              buttons: [{ buttonId: `${prefix}menu`, buttonText: { displayText: "📋 MENU" }, type: 1 }]
            });
          } catch (e) {
            console.error('chr command error', e);
            await sendWithLogoMessage(sender, `❌ Failed to react: ${e.message || e}`, { quoted: msg });
          }
          break;
        }
        
        // ==================== GROUP COMMANDS ====================
        case 'gpromote':
        case 'gdemote':
        case 'gkick':
        case 'gadd': {
          if (!isGroup) {
            await sendWithLogoMessage(sender, `❌ This command can only be used in groups!`, { quoted: msg });
            break;
          }
          
          if (!isOwner) {
            const groupMetadata = await socket.groupMetadata(from);
            const isAdmin = groupMetadata.participants.some(p => p.id === sender && (p.admin === 'admin' || p.admin === 'superadmin'));
            if (!isAdmin) {
              await sendWithLogoMessage(sender, `❌ Only group admins can use this command!`, { quoted: msg });
              break;
            }
          }
          
          let targetUser = null;
          
          // Check if mentioned
          if (msg.message.extendedTextMessage?.contextInfo?.mentionedJid?.length > 0) {
            targetUser = msg.message.extendedTextMessage.contextInfo.mentionedJid[0];
          } 
          // Check if quoted
          else if (msg.message.extendedTextMessage?.contextInfo?.participant) {
            targetUser = msg.message.extendedTextMessage.contextInfo.participant;
          }
          // Check if number provided
          else if (args[0]) {
            let targetNumber = args[0].replace(/[^0-9]/g, '');
            if (!targetNumber.startsWith('94') && targetNumber.length === 9) {
              targetNumber = '94' + targetNumber;
            }
            targetUser = targetNumber + '@s.whatsapp.net';
          }
          
          if (!targetUser) {
            await sendWithLogoMessage(sender, `❌ Please mention or provide a number!\n\nUsage: ${prefix}${command} @user or ${prefix}${command} 94789227570`, { quoted: msg });
            break;
          }
          
          const groupMetadata = await socket.groupMetadata(from);
          const isTargetAdmin = groupMetadata.participants.some(p => p.id === targetUser && (p.admin === 'admin' || p.admin === 'superadmin'));
          
          try {
            switch(command) {
              case 'gpromote':
                if (isTargetAdmin) {
                  await sendWithLogoMessage(sender, `❌ User is already an admin!`, { quoted: msg });
                  break;
                }
                await socket.groupParticipantsUpdate(from, [targetUser], 'promote');
                await sendWithLogoMessage(sender, `✅ *Promoted successfully!*\n\n👤 User: @${targetUser.split('@')[0]}\n👑 Role: Admin`, { mentions: [targetUser], quoted: msg });
                break;
                
              case 'gdemote':
                if (!isTargetAdmin) {
                  await sendWithLogoMessage(sender, `❌ User is not an admin!`, { quoted: msg });
                  break;
                }
                await socket.groupParticipantsUpdate(from, [targetUser], 'demote');
                await sendWithLogoMessage(sender, `✅ *Demoted successfully!*\n\n👤 User: @${targetUser.split('@')[0]}\n👤 Role: Member`, { mentions: [targetUser], quoted: msg });
                break;
                
              case 'gkick':
                await socket.groupParticipantsUpdate(from, [targetUser], 'remove');
                await sendWithLogoMessage(sender, `✅ *Kicked successfully!*\n\n👤 User: @${targetUser.split('@')[0]}\n🚫 Removed from group`, { mentions: [targetUser], quoted: msg });
                break;
                
              case 'gadd':
                await socket.groupParticipantsUpdate(from, [targetUser], 'add');
                await sendWithLogoMessage(sender, `✅ *Added successfully!*\n\n👤 User: @${targetUser.split('@')[0]}\n✨ Added to group`, { mentions: [targetUser], quoted: msg });
                break;
            }
          } catch (e) {
            console.error(`${command} error:`, e);
            await sendWithLogoMessage(sender, `❌ Failed to execute command: ${e.message}`, { quoted: msg });
          }
          break;
        }
        
        // ==================== OWNER COMMANDS ====================
        case 'broadcast': {
          if (!isOwner) {
            await sendWithLogoMessage(sender, `❌ This command is only for the bot owner!`, { quoted: msg });
            break;
          }
          
          const broadcastMessage = args.join(' ');
          if (!broadcastMessage) {
            await sendWithLogoMessage(sender, `❌ Usage: ${prefix}broadcast <message>\n\nSend a message to all active sessions.`, { quoted: msg });
            break;
          }
          
          let sentCount = 0;
          for (const [sessionNumber, sessionSocket] of activeSockets) {
            try {
              const userJid = jidNormalizedUser(sessionSocket.user.id);
              await sessionSocket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: `📢 *BROADCAST*\n\n${broadcastMessage}\n\n> *© ${BOT_NAME_FANCY}*` });
              sentCount++;
              await delay(500);
            } catch (e) {
              console.error(`Failed to send broadcast to ${sessionNumber}:`, e);
            }
          }
          
          await sendWithLogoMessage(sender, `✅ *Broadcast sent!*\n\n📨 Sent to: ${sentCount} sessions\n📝 Message: ${broadcastMessage.substring(0, 100)}`, { quoted: msg });
          break;
        }
        
        case 'delsession':
        case 'deletesession': {
          if (!isOwner) {
            await sendWithLogoMessage(sender, `❌ This command is only for the bot owner!`, { quoted: msg });
            break;
          }
          
          const targetNumber = args[0]?.replace(/[^0-9]/g, '');
          if (!targetNumber) {
            await sendWithLogoMessage(sender, `❌ Usage: ${prefix}delsession <number>\n\nExample: ${prefix}delsession 94789227570\n\n⚠️ This will delete the session permanently!`, { quoted: msg });
            break;
          }
          
          const sessionSocket = activeSockets.get(targetNumber);
          if (sessionSocket) {
            try {
              if (typeof sessionSocket.logout === 'function') await sessionSocket.logout().catch(() => {});
              sessionSocket.ws?.close();
            } catch (e) {}
            activeSockets.delete(targetNumber);
            socketCreationTime.delete(targetNumber);
          }
          
          await removeSessionFromMongo(targetNumber);
          await removeNumberFromMongo(targetNumber);
          
          try {
            const sessTmp = path.join(os.tmpdir(), `session_${targetNumber} `);
            if (fs.existsSync(sessTmp)) fs.removeSync(sessTmp);
          } catch (e) {}
          
          await sendWithLogoMessage(sender, `✅ *Session deleted successfully!*\n\n📞 Number: ${targetNumber}\n🗑️ All data removed.`, { quoted: msg });
          break;
        }
        
        // ==================== SETTINGS MENU - AUTO VIEW ====================
        case 'autoview': {
          const action = args[0]?.toLowerCase();
          if (action === 'on') {
            const newConfig = { ...userConfig, AUTO_VIEW_STATUS: 'true' };
            await setUserConfigInMongo(sanitized, newConfig);
            await sendWithLogoMessage(sender, `✅ *Auto View Status ENABLED!*\n\nBot will automatically view all status updates.`, { quoted: msg });
          } else if (action === 'off') {
            const newConfig = { ...userConfig, AUTO_VIEW_STATUS: 'false' };
            await setUserConfigInMongo(sanitized, newConfig);
            await sendWithLogoMessage(sender, `❌ *Auto View Status DISABLED!*`, { quoted: msg });
          } else {
            await sendWithLogoMessage(sender, `📺 *Auto View Status*\n\nCurrent: ${userConfig.AUTO_VIEW_STATUS === 'true' ? '🟢 ON' : '🔴 OFF'}\n\nUsage: ${prefix}autoview on/off`, { quoted: msg });
          }
          break;
        }
        
        // ==================== SETTINGS MENU - AUTO LIKE ====================
        case 'autolike': {
          const action = args[0]?.toLowerCase();
          if (action === 'on') {
            const newConfig = { ...userConfig, AUTO_LIKE_STATUS: 'true' };
            await setUserConfigInMongo(sanitized, newConfig);
            await sendWithLogoMessage(sender, `✅ *Auto Like Status ENABLED!*\n\nBot will automatically like all status updates.`, { quoted: msg });
          } else if (action === 'off') {
            const newConfig = { ...userConfig, AUTO_LIKE_STATUS: 'false' };
            await setUserConfigInMongo(sanitized, newConfig);
            await sendWithLogoMessage(sender, `❌ *Auto Like Status DISABLED!*`, { quoted: msg });
          } else {
            await sendWithLogoMessage(sender, `❤️ *Auto Like Status*\n\nCurrent: ${userConfig.AUTO_LIKE_STATUS === 'true' ? '🟢 ON' : '🔴 OFF'}\n\nUsage: ${prefix}autolike on/off`, { quoted: msg });
          }
          break;
        }
        
        // ==================== SETTINGS MENU - AUTO RECORD ====================
        case 'autorecord': {
          const action = args[0]?.toLowerCase();
          if (action === 'on') {
            const newConfig = { ...userConfig, AUTO_RECORDING: 'true' };
            await setUserConfigInMongo(sanitized, newConfig);
            await sendWithLogoMessage(sender, `✅ *Auto Recording ENABLED!*\n\nBot will show recording status when typing.`, { quoted: msg });
          } else if (action === 'off') {
            const newConfig = { ...userConfig, AUTO_RECORDING: 'false' };
            await setUserConfigInMongo(sanitized, newConfig);
            await sendWithLogoMessage(sender, `❌ *Auto Recording DISABLED!*`, { quoted: msg });
          } else {
            await sendWithLogoMessage(sender, `🎙️ *Auto Recording*\n\nCurrent: ${userConfig.AUTO_RECORDING === 'true' ? '🟢 ON' : '🔴 OFF'}\n\nUsage: ${prefix}autorecord on/off`, { quoted: msg });
          }
          break;
        }
        
        // ==================== OWNER MENU ====================
        case 'owner': {
          const ownerInfo = `
╭━━━━━━━━━━━━━━━━━━━━╮
┃     *👑 OWNER INFO*     
╰━━━━━━━━━━━━━━━━━━━━╯

📞 *Number:* ${config.OWNER_NUMBER}
👤 *Name:* ${config.OWNER_NAME}
🤖 *Bot:* ${config.BOT_NAME}
📌 *Version:* ${config.BOT_VERSION}
🔗 *Channel:* ${config.CHANNEL_LINK}

💬 *Contact owner for support or inquiries.*
          `;
          await sendWithLogoMessage(sender, ownerInfo, { 
            quoted: msg,
            buttons: [{ buttonId: `${prefix}menu`, buttonText: { displayText: "📋 MENU" }, type: 1 }]
          });
          break;
        }
        
        // ==================== BOT INFO ====================
        case 'botinfo':
        case 'info': {
          const startTime = socketCreationTime.get(number) || Date.now();
          const uptime = Math.floor((Date.now() - startTime) / 1000);
          const hours = Math.floor(uptime / 3600);
          const minutes = Math.floor((uptime % 3600) / 60);
          const seconds = Math.floor(uptime % 60);
          
          const infoText = `
╭━━━━━━━━━━━━━━━━━━━━╮
┃     *🤖 BOT INFO*     
╰━━━━━━━━━━━━━━━━━━━━╯

📛 *Name:* ${config.BOT_NAME}
📌 *Version:* ${config.BOT_VERSION}
👤 *Owner:* ${config.OWNER_NAME}
🎯 *Prefix:* ${config.PREFIX}
📟 *Uptime:* ${hours}h ${minutes}m ${seconds}s
💾 *Sessions:* ${activeSockets.size}
📡 *Status:* 🟢 Online

🔗 *Channel:* ${config.CHANNEL_LINK}
          `;
          await sendWithLogoMessage(sender, infoText, {
            quoted: msg,
            buttons: [
              { buttonId: `${prefix}ping`, buttonText: { displayText: "⚡ PING" }, type: 1 },
              { buttonId: `${prefix}menu`, buttonText: { displayText: "📋 MENU" }, type: 1 }
            ]
          });
          break;
        }
        
        // ==================== PING ====================
        case 'ping': {
          try {
            const start = Date.now();
            const cfg = await loadUserConfigFromMongo(sanitized) || {};
            const botName = cfg.botName || BOT_NAME_FANCY;
            const logo = cfg.logo || config.RCD_IMAGE_PATH;
            const userTag = `@${sender.split("@")[0]} `;

            const now = new Date();
            const sriLankaTime = now.toLocaleString('en-US', { timeZone: 'Asia/Colombo' });
            const sriLankaDate = new Date(sriLankaTime);
            const currentHour = sriLankaDate.getHours();

            let greeting;
            if (currentHour >= 5 && currentHour < 12) {
              greeting = 'Good Morning 🌅';
            } else if (currentHour >= 12 && currentHour < 18) {
              greeting = 'Good Afternoon ☀️';
            } else {
              greeting = 'Good Evening 🌙';
            }

            const formattedTime = sriLankaDate.toLocaleTimeString('en-US', {
              hour: '2-digit',
              minute: '2-digit',
              second: '2-digit',
              hour12: true,
              timeZone: 'Asia/Colombo'
            });

            const end = Date.now();
            const latency = end - start;

            const speedStatus = latency < 200 ? 'Excellent 🟢' : latency < 500 ? 'Good 🟡' : 'Slow 🔴';

            const text = `
🏓 *PING RESULT*

👤 USER: ${userTag}
🗯️ GREETING: ${greeting}
⏰ TIME: ${formattedTime}

⚡ SPEED: ${latency} ms
📡 STATUS: ${speedStatus}

Thanks for using ${botName} 🚀
`;

            let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

            const buttons = [
              { buttonId: `${prefix}menu`, buttonText: { displayText: "📋 MENU" }, type: 1 },
              { buttonId: `${prefix}alive`, buttonText: { displayText: "🤖 ALIVE" }, type: 1 }
            ];

            await socket.sendMessage(sender, {
              image: imagePayload,
              caption: text,
              footer: `*${botName}*`,
              buttons: buttons,
              headerType: 4,
              mentions: [sender]
            }, { quoted: msg });
          } catch (e) {
            console.error('ping error', e);
            await sendWithLogoMessage(sender, '❌ Failed to test ping.', { quoted: msg });
          }
          break;
        }
        
        // ==================== ALIVE ====================
        case 'alive': {
          try {
            const cfg = await loadUserConfigFromMongo(sanitized) || {};
            const botName = cfg.botName || BOT_NAME_FANCY;
            const logo = cfg.logo || config.RCD_IMAGE_PATH;

            const now = new Date();
            const sriLankaTime = now.toLocaleString('en-US', { timeZone: 'Asia/Colombo' });
            const sriLankaDate = new Date(sriLankaTime);
            const currentHour = sriLankaDate.getHours();

            let greeting;
            if (currentHour >= 5 && currentHour < 12) {
              greeting = 'Good Morning 🌅';
            } else if (currentHour >= 12 && currentHour < 18) {
              greeting = 'Good Afternoon';
            } else {
              greeting = 'Good Evening 🌙';
            }

            const optionsDate = { month: 'long', day: 'numeric', timeZone: 'Asia/Colombo' };
            const formattedDate = sriLankaDate.toLocaleDateString('en-US', optionsDate);

            const optionsDay = { weekday: 'long', timeZone: 'Asia/Colombo' };
            const formattedDay = sriLankaDate.toLocaleDateString('en-US', optionsDay);

            const optionsTime = { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true, timeZone: 'Asia/Colombo' };
            const formattedTime = sriLankaDate.toLocaleTimeString('en-US', optionsTime);

            const metaQuote = {
              key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ALIVE" },
              message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
            };

            const vnoteUrl = 'https://whiteshadow-uploader.vercel.app/files/20n.mp4';
            await socket.sendMessage(sender, {
              video: { url: vnoteUrl },
              ptv: true
            }, { quoted: metaQuote });

            await delay(500);

            const startTime = socketCreationTime.get(number) || Date.now();
            const uptime = Math.floor((Date.now() - startTime) / 1000);
            const hours = Math.floor(uptime / 3600);
            const minutes = Math.floor((uptime % 3600) / 60);
            const seconds = Math.floor(uptime % 60);

            const text = `
*𝗛ɪ 👋 © ${botName}*

╭───────────────────╮
┃ 🗯️ GREETING : ${greeting}
┃ 🗓️ DATE : ${formattedDate}
┃ 📆 DAY : ${formattedDay}
┃ ⏱️ TIME : ${formattedTime} (IST)
┃ 📄 BOT NAME : ${config.BOT_NAME}
┃ 🥷 OWNER : ${config.OWNER_NAME}
┃ 🧬 VERSION : ${config.BOT_VERSION}
┃ 📟 UPTIME : ${hours}h ${minutes}m ${seconds}s
┃ ✒️ PREFIX : ${config.PREFIX}
╰───────────────────╯
`;

            const buttons = [
              {
                buttonId: 'action',
                buttonText: { displayText: 'Menu Options' },
                type: 4,
                nativeFlowInfo: {
                  name: 'single_select',
                  paramsJson: JSON.stringify({
                    title: 'Click Here',
                    sections: [{
                      title: `${botName}`,
                      rows: [
                        { title: 'Alive', description: 'Get Bot Status', id: `${config.PREFIX}alive` },
                        { title: 'Ping', description: 'Get Bot Speed', id: `${config.PREFIX}ping` },
                        { title: 'Menu', description: 'Main Menu', id: `${config.PREFIX}menu` }
                      ]
                    }]
                  })
                }
              }
            ];

            let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

            await socket.sendMessage(sender, {
              image: imagePayload,
              caption: text,
              footer: `*${botName}*`,
              buttons,
              headerType: 4
            }, { quoted: metaQuote });
          } catch (e) {
            console.error('alive error', e);
            await sendWithLogoMessage(sender, '❌ Failed to send alive status.', { quoted: msg });
          }
          break;
        }
        
        // ==================== SYSTEM ====================
        case 'system': {
          try {
            const cfg = await loadUserConfigFromMongo(sanitized) || {};
            const botName = cfg.botName || BOT_NAME_FANCY;
            const logo = cfg.logo || config.RCD_IMAGE_PATH;

            const os = require('os');
            const text = `
☘️ *System Info for ${botName}*

╭━━━━━━━━━━━━━━━━━━━━╮
┃ 🧸 OS: ${os.type()} ${os.release()}
┃ 📡 PLATFORM: ${os.platform()}
┃ 🧠 CPU CORES: ${os.cpus().length}
┃ 💾 MEMORY: ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(2)} GB
┃ 🖥️ HOST: ${os.hostname()}
╰━━━━━━━━━━━━━━━━━━━━╯
            `;

            let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

            await socket.sendMessage(sender, {
              image: imagePayload,
              caption: text,
              footer: `*${botName}*`,
              buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📋 MENU" }, type: 1 }],
              headerType: 4
            }, { quoted: msg });
          } catch (e) {
            console.error('system error', e);
            await sendWithLogoMessage(sender, '❌ Failed to get system info.', { quoted: msg });
          }
          break;
        }
        
        default:
          // Unknown command - check if it's a number reply for submenu
          if (body.match(/^\d+$/) && !command) {
            const num = parseInt(body);
            if (num >= 1 && num <= 10) {
              await showSubMenu(socket, sender, num, sanitized, msg);
            }
          }
          break;
      }
    } catch (err) {
      console.error('Command handler error:', err);
      try {
        await sendWithLogoMessage(sender, '❌ An error occurred while processing your command. Please try again.', { quoted: msg });
      } catch (e) { }
    }
  });
}

// ---------------- Call Rejection Handler ----------------

async function setupCallRejection(socket, sessionNumber) {
  socket.ev.on('call', async (calls) => {
    try {
      const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      if (userConfig.ANTI_CALL !== 'on') return;

      console.log(`📞 Incoming call detected for ${sanitized} - Auto rejecting...`);

      for (const call of calls) {
        if (call.status !== 'offer') continue;

        const id = call.id;
        const from = call.from;

        await socket.rejectCall(id, from);
        await socket.sendMessage(from, { text: '*🔕 Auto call rejection is enabled. Calls are automatically rejected.*' });
        console.log(`✅ Auto - rejected call from ${from}`);

        const userJid = jidNormalizedUser(socket.user.id);
        const rejectionMessage = formatMessage('📞 CALL REJECTED', `Auto call rejection is active.\n\nCall from: ${from}\nTime: ${getSriLankaTimestamp()}`, BOT_NAME_FANCY);

        await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: rejectionMessage });
      }
    } catch (err) {
      console.error(`Call rejection error for ${sessionNumber}: `, err);
    }
  });
}

// ---------------- Auto Message Read Handler ----------------

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

      if (type === 'conversation') {
        body = actualMsg.conversation || '';
      } else if (type === 'extendedTextMessage') {
        body = actualMsg.extendedTextMessage?.text || '';
      }
    } catch (e) {
      body = '';
    }

    const prefix = userConfig.PREFIX || config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);

    if (autoReadSetting === 'all') {
      try {
        await socket.readMessages([msg.key]);
        console.log(`✅ Message read: ${msg.key.id}`);
      } catch (error) {
        console.warn('Failed to read message:', error?.message);
      }
    } else if (autoReadSetting === 'cmd' && isCmd) {
      try {
        await socket.readMessages([msg.key]);
        console.log(`✅ Command message read: ${msg.key.id}`);
      } catch (error) {
        console.warn('Failed to read command message:', error?.message);
      }
    }
  });
}

// ---------------- message handlers ----------------

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
        try {
          await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
          setTimeout(async () => {
            try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) { }
          }, 3000);
        } catch (e) {
          console.error('Auto typing error:', e);
        }
      }

      if (autoRecording === 'true') {
        try {
          await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
          setTimeout(async () => {
            try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) { }
          }, 3000);
        } catch (e) {
          console.error('Auto recording error:', e);
        }
      }
    } catch (error) {
      console.error('Message handler error:', error);
    }
  });
}

// ---------------- cleanup helper ----------------

async function deleteSessionAndCleanup(number, socketInstance) {
  const sanitized = number.replace(/[^0-9]/g, '');
  try {
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized} `);
    try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch (e) { }
    activeSockets.delete(sanitized);
    socketCreationTime.delete(sanitized);
    try { await removeSessionFromMongo(sanitized); } catch (e) { }
    try { await removeNumberFromMongo(sanitized); } catch (e) { }
    try {
      const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
      const caption = formatMessage('*🥷 OWNER NOTICE — SESSION REMOVED*', `*Number:* ${sanitized}\n*Session Removed Due To Logout.*\n\n*Active Sessions Now:* ${activeSockets.size}`, BOT_NAME_FANCY);
      if (socketInstance && socketInstance.sendMessage) await socketInstance.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
    } catch (e) { }
    console.log(`Cleanup completed for ${sanitized}`);
  } catch (err) { console.error('deleteSessionAndCleanup error:', err); }
}

// ---------------- auto-restart ----------------

function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode
        || lastDisconnect?.error?.statusCode
        || (lastDisconnect?.error && lastDisconnect.error.toString().includes('401') ? 401 : undefined);
      const isLoggedOut = statusCode === 401
        || (lastDisconnect?.error && lastDisconnect.error.code === 'AUTHENTICATION')
        || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('logged out'))
        || (lastDisconnect?.reason === DisconnectReason?.loggedOut);
      if (isLoggedOut) {
        console.log(`User ${number} logged out.Cleaning up...`);
        try { await deleteSessionAndCleanup(number, socket); } catch (e) { console.error(e); }
      } else {
        console.log(`Connection closed for ${number}(not logout).Attempt reconnect...`);
        try {
          await delay(10000);
          activeSockets.delete(number.replace(/[^0-9]/g, ''));
          socketCreationTime.delete(number.replace(/[^0-9]/g, ''));
          const mockRes = { headersSent: false, send: () => { }, status: () => mockRes };
          await EmpirePair(number, mockRes);
        } catch (e) { console.error('Reconnect attempt failed', e); }
      }
    }
  });
}

// ---------------- EmpirePair (pairing, temp dir, persist to Mongo) ----------------

async function EmpirePair(number, res) {
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const sessionPath = path.join(os.tmpdir(), `session_${sanitizedNumber} `);
  await initMongo().catch(() => { });

  try {
    const mongoDoc = await loadCredsFromMongo(sanitizedNumber);
    if (mongoDoc && mongoDoc.creds) {
      fs.ensureDirSync(sessionPath);
      fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(mongoDoc.creds, null, 2));
      if (mongoDoc.keys) fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(mongoDoc.keys, null, 2));
      console.log('Prefilled creds from Mongo');
    }
  } catch (e) { console.warn('Prefill from Mongo failed', e); }

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
    setupVVMessageHandler(socket, sanitizedNumber);

    if (!socket.authState.creds.registered) {
      let retries = config.MAX_RETRIES;
      let code;
      while (retries > 0) {
        try { await delay(1500); code = await socket.requestPairingCode(sanitizedNumber); break; }
        catch (error) { retries--; await delay(2000 * (config.MAX_RETRIES - retries)); }
      }
      if (!res.headersSent) res.send({ code });
    }

    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        const credsPath = path.join(sessionPath, 'creds.json');
        if (!fs.existsSync(credsPath)) return;
        const fileStats = fs.statSync(credsPath);
        if (fileStats.size === 0) return;
        const fileContent = await fs.readFile(credsPath, 'utf8');
        const trimmedContent = fileContent.trim();
        if (!trimmedContent || trimmedContent === '{}' || trimmedContent === 'null') return;
        let credsObj;
        try { credsObj = JSON.parse(trimmedContent); } catch (e) { return; }
        if (!credsObj || typeof credsObj !== 'object') return;
        const keysObj = state.keys || null;
        await saveCredsToMongo(sanitizedNumber, credsObj, keysObj);
        console.log('✅ Creds saved to MongoDB successfully');
      } catch (err) {
        console.error('Failed saving creds on creds.update:', err);
      }
    });

    socket.ev.on('connection.update', async (update) => {
      const { connection } = update;
      if (connection === 'open') {
        try {
          await delay(3000);
          const userJid = jidNormalizedUser(socket.user.id);
          const groupResult = await joinGroup(socket).catch(() => ({ status: 'failed', error: 'joinGroup not configured' }));

          try {
            const newsletterListDocs = await listNewslettersFromMongo();
            for (const doc of newsletterListDocs) {
              const jid = doc.jid;
              try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jid); } catch (e) { }
            }
          } catch (e) { }

          activeSockets.set(sanitizedNumber, socket);
          const groupStatus = groupResult.status === 'success' ? 'Joined successfully' : `Failed to join group: ${groupResult.error} `;

          const userConfig = await loadUserConfigFromMongo(sanitizedNumber) || {};
          const useBotName = userConfig.botName || BOT_NAME_FANCY;
          const useLogo = config.RCD_IMAGE_PATH;

          const initialCaption = formatMessage(useBotName,
            `*✅ Successfully Connected ✅*\n\n*Number:* ${sanitizedNumber}\n*Connecting:* Wait few seconds`,
            useBotName
          );

          let sentMsg = null;
          try {
            if (String(useLogo).startsWith('http')) {
              sentMsg = await socket.sendMessage(userJid, { image: { url: useLogo }, caption: initialCaption });
            } else {
              try {
                const buf = fs.readFileSync(useLogo);
                sentMsg = await socket.sendMessage(userJid, { image: buf, caption: initialCaption });
              } catch (e) {
                sentMsg = await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: initialCaption });
              }
            }
          } catch (e) {
            try { sentMsg = await socket.sendMessage(userJid, { text: initialCaption }); } catch (e) { }
          }

          await delay(4000);

          const updatedCaption = formatMessage(useBotName,
            `*✅ Successfully Connected ✅*\n\n*Number:* ${sanitizedNumber}\n*Status:* ${groupStatus}\n*Connect Time:* ${getSriLankaTimestamp()}`,
            useBotName
          );

          try {
            if (sentMsg && sentMsg.key) {
              try { await socket.sendMessage(userJid, { delete: sentMsg.key }); } catch (delErr) { }
            }
            try {
              if (String(useLogo).startsWith('http')) {
                await socket.sendMessage(userJid, { image: { url: useLogo }, caption: updatedCaption });
              } else {
                try {
                  const buf = fs.readFileSync(useLogo);
                  await socket.sendMessage(userJid, { image: buf, caption: updatedCaption });
                } catch (e) {
                  await socket.sendMessage(userJid, { text: updatedCaption });
                }
              }
            } catch (imgErr) {
              await socket.sendMessage(userJid, { text: updatedCaption });
            }
          } catch (e) { }

          await sendAdminConnectMessage(socket, sanitizedNumber, groupResult, userConfig);
          await sendOwnerConnectMessage(socket, sanitizedNumber, groupResult, userConfig);
          await addNumberToMongo(sanitizedNumber);
        } catch (e) {
          console.error('Connection open error:', e);
          try { exec(`pm2.restart ${process.env.PM2_NAME || 'CHATUWA-MINI-main'} `); } catch (e) { }
        }
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

// ---------------- endpoints ----------------

router.post('/newsletter/add', async (req, res) => {
  const { jid, emojis } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  if (!jid.endsWith('@newsletter')) return res.status(400).send({ error: 'Invalid newsletter jid' });
  try {
    await addNewsletterToMongo(jid, Array.isArray(emojis) ? emojis : []);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.post('/newsletter/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeNewsletterFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.get('/newsletter/list', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.status(200).send({ status: 'ok', channels: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.post('/admin/add', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await addAdminToMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.post('/admin/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeAdminFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.get('/admin/list', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.status(200).send({ status: 'ok', admins: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) return res.status(400).send({ error: 'Number parameter is required' });
  if (activeSockets.has(number.replace(/[^0-9]/g, ''))) return res.status(200).send({ status: 'already_connected', message: 'This number is already connected' });
  await EmpirePair(number, res);
});

router.get('/active', (req, res) => {
  res.status(200).send({ botName: BOT_NAME_FANCY, count: activeSockets.size, numbers: Array.from(activeSockets.keys()), timestamp: getSriLankaTimestamp() });
});

router.get('/ping', (req, res) => {
  res.status(200).send({ status: 'active', botName: BOT_NAME_FANCY, message: 'lakshan md BOT', activesession: activeSockets.size });
});

router.get('/connect-all', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No numbers found to connect' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => { }, status: () => mockRes };
      await EmpirePair(number, mockRes);
      results.push({ number, status: 'connection_initiated' });
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Connect all error:', error); res.status(500).send({ error: 'Failed to connect all bots' }); }
});

router.get('/reconnect', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No session numbers found in MongoDB' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => { }, status: () => mockRes };
      try { await EmpirePair(number, mockRes); results.push({ number, status: 'connection_initiated' }); } catch (err) { results.push({ number, status: 'failed', error: err.message }); }
      await delay(1000);
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Reconnect error:', error); res.status(500).send({ error: 'Failed to reconnect bots' }); }
});

router.get('/update-config', async (req, res) => {
  const { number, config: configString } = req.query;
  if (!number || !configString) return res.status(400).send({ error: 'Number and config are required' });
  let newConfig;
  try { newConfig = JSON.parse(configString); } catch (error) { return res.status(400).send({ error: 'Invalid config format' }); }
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const otp = generateOTP();
  otpStore.set(sanitizedNumber, { otp, expiry: Date.now() + config.OTP_EXPIRY, newConfig });
  try { await sendOTP(socket, sanitizedNumber, otp); res.status(200).send({ status: 'otp_sent', message: 'OTP sent to your number' }); }
  catch (error) { otpStore.delete(sanitizedNumber); res.status(500).send({ error: 'Failed to send OTP' }); }
});

router.get('/verify-otp', async (req, res) => {
  const { number, otp } = req.query;
  if (!number || !otp) return res.status(400).send({ error: 'Number and OTP are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const storedData = otpStore.get(sanitizedNumber);
  if (!storedData) return res.status(400).send({ error: 'No OTP request found for this number' });
  if (Date.now() >= storedData.expiry) { otpStore.delete(sanitizedNumber); return res.status(400).send({ error: 'OTP has expired' }); }
  if (storedData.otp !== otp) return res.status(400).send({ error: 'Invalid OTP' });
  try {
    await setUserConfigInMongo(sanitizedNumber, storedData.newConfig);
    otpStore.delete(sanitizedNumber);
    const sock = activeSockets.get(sanitizedNumber);
    if (sock) await sock.sendMessage(jidNormalizedUser(sock.user.id), { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('📌 CONFIG UPDATED', 'Your configuration has been successfully updated!', BOT_NAME_FANCY) });
    res.status(200).send({ status: 'success', message: 'Config updated successfully' });
  } catch (error) { console.error('Failed to update config:', error); res.status(500).send({ error: 'Failed to update config' }); }
});

router.get('/getabout', async (req, res) => {
  const { number, target } = req.query;
  if (!number || !target) return res.status(400).send({ error: 'Number and target number are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const targetJid = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
  try {
    const statusData = await socket.fetchStatus(targetJid);
    const aboutStatus = statusData.status || 'No status available';
    const setAt = statusData.setAt ? moment(statusData.setAt).tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss') : 'Unknown';
    res.status(200).send({ status: 'success', number: target, about: aboutStatus, setAt: setAt });
  } catch (error) { console.error(`Failed to fetch status for ${target}: `, error); res.status(500).send({ status: 'error', message: `Failed to fetch About status for ${target}.` }); }
});

// ---------------- Dashboard endpoints ----------------

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
  } catch (err) {
    console.error('API /api/sessions error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});

router.get('/api/active', async (req, res) => {
  try {
    const keys = Array.from(activeSockets.keys());
    res.json({ ok: true, active: keys, count: keys.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});

router.post('/api/session/delete', async (req, res) => {
  try {
    const { number } = req.body;
    if (!number) return res.status(400).json({ ok: false, error: 'number required' });
    const sanitized = ('' + number).replace(/[^0-9]/g, '');
    const running = activeSockets.get(sanitized);
    if (running) {
      try { if (typeof running.logout === 'function') await running.logout().catch(() => { }); } catch (e) { }
      try { running.ws?.close(); } catch (e) { }
      activeSockets.delete(sanitized);
      socketCreationTime.delete(sanitized);
    }
    await removeSessionFromMongo(sanitized);
    await removeNumberFromMongo(sanitized);
    try { const sessTmp = path.join(os.tmpdir(), `session_${sanitized} `); if (fs.existsSync(sessTmp)) fs.removeSync(sessTmp); } catch (e) { }
    res.json({ ok: true, message: `Session ${sanitized} removed` });
  } catch (err) {
    console.error('API /api/session/delete error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});

router.get('/api/newsletters', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.json({ ok: true, list });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});

router.get('/api/admins', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.json({ ok: true, list });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});

// ---------------- cleanup + process events ----------------

process.on('exit', () => {
  activeSockets.forEach((socket, number) => {
    try { socket.ws.close(); } catch (e) { }
    activeSockets.delete(number);
    socketCreationTime.delete(number);
    try { fs.removeSync(path.join(os.tmpdir(), `session_${number} `)); } catch (e) { }
  });
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
  try { exec(`pm2.restart ${process.env.PM2_NAME || 'CHATUWA-MINI-main'} `); } catch (e) { console.error('Failed to restart pm2:', e); }
});

initMongo().then(async () => {
  try {
    const credsJson = process.env.CREDS_JSON || config.CREDS_JSON;
    const sessionId = process.env.SESSION_ID || config.SESSION_ID;
    const ownerNumber = (config.OWNER_NUMBER || '').replace(/[^0-9]/g, '');

    if (ownerNumber) {
      let creds = null;
      if (credsJson) {
        console.log('Found CREDS_JSON in environment variables.');
        creds = JSON.parse(credsJson);
      } else if (sessionId) {
        console.log(`Found SESSION_ID(${sessionId}) in environment variables.Fetching...`);
        const url = sessionId.startsWith('http') ? sessionId : `https://files.catbox.moe/${sessionId}`;
        const resp = await axios.get(url);
        creds = resp.data;
      }
      if (creds && typeof creds === 'object') {
        await saveCredsToMongo(ownerNumber, creds);
        console.log(`✅ Loaded and saved session from ENV for ${ownerNumber}`);
      }
    }
  } catch (e) {
    console.error('Error loading session from env:', e.message);
  }

  try {
    const nums = await getAllNumbersFromMongo();
    if (nums && nums.length) {
      for (const n of nums) {
        if (!activeSockets.has(n)) {
          const mockRes = { headersSent: false, send: () => { }, status: () => mockRes };
          await EmpirePair(n, mockRes);
          await delay(500);
        }
      }
    }
  } catch (e) { }
}).catch(err => console.warn('Mongo init failed at startup', err));

module.exports = router;