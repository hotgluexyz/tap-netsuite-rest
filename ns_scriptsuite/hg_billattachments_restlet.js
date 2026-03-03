/**
 * @NApiVersion 2.1
 * @NScriptType Restlet
 */
define(['N/record', 'N/search', 'N/error', 'N/log', 'N/cache', 'N/crypto'],
  (record, search, error, log, cache, crypto) => {

    // Hardcoded salt is acceptable here: this script runs server-side in NetSuite only,
    // and script source is visible only to NetSuite administrators. Not exposed to clients.
    // Must match the salt in hg_billattachments_suitelet.js.
    const INTERNAL_SALT = 'CHANGE_ME';

    function getBillAttachments(requestPost) {
      if (!requestPost.requestId || !requestPost.vendorBillIds || !Array.isArray(requestPost.vendorBillIds)) {
        throw error.create({
          name: 'MISSING_PARAMS',
          message: 'requestId and vendorBillIds (array) are required.',
          status: 400
        });
      }

      const { requestId, vendorBillIds } = requestPost;

      try {
        // 1. Generate the Signed Download Token
        const sha256 = crypto.createHash({ algorithm: crypto.HashAlg.SHA256 });
        sha256.update({ input: requestId + INTERNAL_SALT });
        const downloadToken = sha256.digest({ outputEncoding: crypto.Encoding.HEX });

        // 2. Perform Search
        const transactionSearch = search.create({
          type: record.Type.VENDOR_BILL,
          filters: [['internalid', 'anyof', vendorBillIds]],
          columns: [
            search.createColumn({ name: 'internalid', join: 'file' }),
            search.createColumn({ name: 'name', join: 'file' }),
            search.createColumn({ name: 'filetype', join: 'file' }),
            search.createColumn({ name: 'url', join: 'file' }),
            'internalid', 'tranid', 'trandate'
          ]
        });

        const items = [];
        const fileIdsForCache = [];
        const attachmentsMap = {};

        transactionSearch.run().each(result => {
          const vendorBillId = result.getValue('internalid');
          const fileId = result.getValue({ name: 'internalid', join: 'file' });

          if (fileId && !attachmentsMap[`${vendorBillId}_${fileId}`]) {
            fileIdsForCache.push(fileId);
            items.push({
              tranid: result.getValue('tranid'),
              trandate: result.getValue('trandate'),
              transaction: vendorBillId,
              file_id: fileId,
              file_name: result.getValue({ name: 'name', join: 'file' }),
              file_type: result.getValue({ name: 'filetype', join: 'file' }),
              file_url: result.getValue({ name: 'url', join: 'file' })
            });
            attachmentsMap[`${vendorBillId}_${fileId}`] = true;
          }
          return true;
        });

        // 3. Store in PUBLIC Cache (Expires in 30 minutes)
        // We store the authorized file list so the Suitelet can verify it.
        const downloadCache = cache.getCache({
          name: 'ATTACHMENT_DOWNLOAD_CACHE',
          scope: cache.Scope.PUBLIC
        });

        downloadCache.put({
          key: requestId,
          value: JSON.stringify({
            token: downloadToken,
            authorizedFiles: fileIdsForCache
          }),
          ttl: 1800
        });

        return {
          download_token: downloadToken,
          items: items
        };

      } catch (e) {
        log.error('RESTlet Error', e.message);
        throw error.create({ name: 'RESTLET_ERR', message: e.message, status: 500 });
      }
    }

    return { post: getBillAttachments };
  });