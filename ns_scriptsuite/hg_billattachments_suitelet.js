/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 */
define(['N/file', 'N/log', 'N/error', 'N/cache', 'N/crypto'], function (file, log, error, cache, crypto) {
  
  // HARDCODED SALT: Stays on the server. Change this to a unique string for your account.
  // SALT needs to be in both the restlet and the suitelet.
  const INTERNAL_SALT = 'CHANGE_ME';

  /**
   * Raises an error: if request has checkerror param, writes JSON { code, message } and returns;
   * otherwise throws N/error.
   * @param {Object} context - scriptContext (request + response)
   * @param {Object} opts - { name, message, status?, notifyOff? }
   */
  function raiseError(context, opts) {
    var name = opts.name;
    var message = opts.message;
    var status = opts.status || 400;
    var notifyOff = opts.notifyOff !== undefined ? opts.notifyOff : true;

    if (context.request.parameters.checkerror) {
      context.response.setHeader({ name: 'Content-Type', value: 'application/json' });
      context.response.write(JSON.stringify({ code: name, message: message }));
      return;
    }

    throw error.create({
      name: name,
      message: message,
      status: status,
      notifyOff: notifyOff
    });
  }

  function computeExpectedToken(requestId) {
    const sha256 = crypto.createHash({ algorithm: crypto.HashAlg.SHA256 });
    sha256.update({ input: requestId + INTERNAL_SALT });
    return sha256.digest({ outputEncoding: crypto.Encoding.HEX });
  }

  function onRequest(context) {
    if (context.request.method === 'GET') {
      const fileId = context.request.parameters.fileId;
      const requestId = context.request.parameters.requestId;
      const downloadToken = context.request.parameters.download_token;

      if (!fileId || !requestId || !downloadToken) {
        raiseError(context, {
          name: 'INVALID_PARAMETERS',
          message: 'fileId, requestId, and download_token are required.',
          notifyOff: true
        });
        return;
      }

      try {
        // 1. Validate token against signature: recompute SHA256(requestId + SALT) and compare
        const expectedToken = computeExpectedToken(requestId);
        if (downloadToken !== expectedToken) {
          raiseError(context, {
            name: 'INVALID_TOKEN',
            message: 'Invalid download token.',
            notifyOff: true
          });
          return;
        }

        // 2. Load cache to get the list of authorized file IDs for this request
        const downloadCache = cache.getCache({
          name: 'ATTACHMENT_DOWNLOAD_CACHE',
          scope: cache.Scope.PUBLIC
        });

        const cached = downloadCache.get({ key: requestId });
        if (!cached) {
          raiseError(context, {
            name: 'INVALID_OR_EXPIRED_LINK',
            message: 'Invalid or expired download link. Please request attachments again.',
            notifyOff: true
          });
          return;
        }

        const { authorizedFiles } = JSON.parse(cached);

        const fileIdStr = String(fileId);
        const isAuthorized = authorizedFiles && authorizedFiles.some(function (id) { return String(id) === fileIdStr; });
        if (!isAuthorized) {
          raiseError(context, {
            name: 'UNAUTHORIZED_FILE',
            message: 'This file is not authorized for download with this request.',
            notifyOff: true
          });
          return;
        }

        const fileObj = file.load({
          id: fileId
        });

        // Write the file content directly to the response
        context.response.writeFile({
          file: fileObj,
          isstream: true
        });

      } catch (e) {
        var ourErrors = ['INVALID_PARAMETERS', 'INVALID_TOKEN', 'INVALID_OR_EXPIRED_LINK', 'UNAUTHORIZED_FILE', 'METHOD_NOT_ALLOWED', 'NOT_FOUND', 'INTERNAL_ERROR'];
        if (e.name && e.message && ourErrors.indexOf(e.name) !== -1) {
          throw e;
        }
        log.error({
          title: 'Download File Error',
          details: 'Failed to download file with ID ' + fileId + ': ' + e.message
        });
        var msg = (e.message || '').toLowerCase();
        var isNotFound = msg.indexOf('not found') !== -1 || msg.indexOf('does not exist') !== -1 || msg.indexOf('invalid id') !== -1 || e.code === 'RECORD_NOT_FOUND';
        raiseError(context, {
          name: isNotFound ? 'NOT_FOUND' : 'INTERNAL_ERROR',
          message: isNotFound ? 'File not found.' : 'Failed to download file.',
          status: isNotFound ? 404 : 500,
          notifyOff: true
        });
      }
    } else {
      raiseError(context, {
        name: 'METHOD_NOT_ALLOWED',
        message: 'This endpoint only supports GET requests.',
        status: 405,
        notifyOff: true
      });
    }
  }

  return {
    onRequest: onRequest
  };
});
