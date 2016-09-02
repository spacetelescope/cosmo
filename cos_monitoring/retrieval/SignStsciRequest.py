# Copyright (C) 2014 Association of Universities for Research in Astronomy (AURA)
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials provided
#      with the distribution.
#
#    3. The name of AURA and its representatives may not be used to
#      endorse or promote products derived from this software without
#      specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY AURA ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL AURA BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
#
# $Log: SignStsciRequest.py,v $
# Revision 1.6  2015/06/29 19:32:17  fred
# PR 80802 remove requirement that xml work in python
#
# Revision 1.5  2014/08/14 19:11:59  fred
# PR 75462 change class name, add rcs log header
#

usexml=True
try:
  import xmlsec
  import libxml2
except ImportError:
  usexml=False
try:
    from urllib.request import Request
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode
    from urllib2 import Request

from six.moves.urllib.request import urlopen
import warnings
import re

class SignStsciRequest:

    def __init__(self):
        self.init()

    @staticmethod
    def init():
        global usexml
        if not usexml:
            return
        try:
          # Init libxml library
          libxml2.initParser()
          libxml2.substituteEntitiesDefault(1)
          # Init xmlsec library
          assert xmlsec.init() >= 0, "Error: xmlsec initialization failed."
          # Check loaded library version
          assert xmlsec.checkVersion() == 1, "Error: loaded xmlsec library version is not compatible."
          # Init crypto library
          assert xmlsec.cryptoAppInit(None) >= 0, "Error: crypto initialization failed."
          # Init xmlsec-crypto library
          assert xmlsec.cryptoInit() >= 0, "Error: xmlsec-crypto initialization failed."
        except:
          usexml=False

    @staticmethod
    def signRequest(file, request, dtd="http://dmswww.stsci.edu/dtd/sso/distribution.dtd", cgi="https://archive.stsci.edu/cgi-bin/dads.cgi", mission='HST'):
     global usexml
     if usexml:
      try:
        keysmngr = xmlsec.KeysMngr()
        if keysmngr is None:
            raise RuntimeError("Error: failed to create keys manager.")
        if xmlsec.cryptoAppDefaultKeysMngrInit(keysmngr) < 0:
            keysmngr.destroy()
            raise RuntimeError("Error: failed to initialize keys manager.")

        key = xmlsec.cryptoAppKeyLoad(filename = file, pwd = None,
           format = xmlsec.KeyDataFormatPem, pwdCallback = None,
           pwdCallbackCtx = None)
        if xmlsec.cryptoAppDefaultKeysMngrAdoptKey(keysmngr, key) < 0:
            keysmngr.destroy()
            raise RuntimeError("Error: failed to load key into keys manager")

        dsig_ctx = xmlsec.DSigCtx(keysmngr)

        # Match the dtd and replace it.

        pat = re.compile("(^.*<!DOCTYPE.*distributionRequest[^>]*SYSTEM[ \t]*\")([^\"]*)(\"[^>]*>.*$)", re.DOTALL)
        m = pat.match(request)

        request = m.group(1)+dtd+m.group(3)

        ctxt = libxml2.createMemoryParserCtxt(request, len(request))
        ctxt.validate(1)
        ctxt.parseDocument()
        doc = ctxt.doc()

        if doc is None or doc.getRootElement() is None:
            keysmngr.destroy()
            raise RuntimeError("Error: unable to parse XML data")

        # find the XML-DSig start node
        node = xmlsec.findNode(doc.getRootElement(), xmlsec.NodeSignature,
                       xmlsec.DSigNs)
        if node is None:
            fragment = libxml2.parseDoc("""<Signature xmlns="http://www.w3.org/2000/09/xmldsig#">
<SignedInfo>
  <CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>
  <SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>
  <Reference URI="#distributionRequest">
    <Transforms>
      <Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>
      <Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#WithComments" />
    </Transforms>
    <DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>
    <DigestValue></DigestValue>
  </Reference>
</SignedInfo>
<SignatureValue></SignatureValue>
<KeyInfo>
 <KeyValue><RSAKeyValue>
  <Modulus></Modulus>
  <Exponent></Exponent>
 </RSAKeyValue></KeyValue>
</KeyInfo>
</Signature>
""")
            # remove the xml header on the front of the document fragment
            fragment = fragment.getRootElement()
            # getElementsByTagName doesn't exist here for some reason, have to use xpath
            ctxt = doc.xpathNewContext()
            nodeList = ctxt.xpathEval("/distributionRequest")
            for child in nodeList:
                child.addChild(fragment)
                if child.prop('Id') == None:
                    child.setProp('Id', 'distributionRequest')
            node = xmlsec.findNode(doc.getRootElement(), xmlsec.NodeSignature,
                       xmlsec.DSigNs)


        # Remove passwords

        ctxt = doc.xpathNewContext()
        nodeList = ctxt.xpathEval('//requester')
        for child in nodeList:
            child.unsetProp('archivePassword')

        nodeList = ctxt.xpathEval('//ftp')
        for child in nodeList:
            if child.hasProp('loginPassword'):
                child.unsetProp('loginPassword')
                warnings.warn('ftp password is not allowed in user requests.')

        # Sign the template, or resign existing block
        status = dsig_ctx.sign(node)
        output = str(doc)
        doc.freeDoc()
        keysmngr.destroy()
        if status < 0:
            raise RuntimeError("Error: signature failed")
        return output
      except:
        usexml=False
        return signRequest(file, request, dtd, cgi)
     else:
        values = {'request' : request, 'privatekey' : open(file).read(), 'mission' : mission }
        data = urlencode(values)
        req = Request(url=cgi, data=data)
        f = urlopen(req)
        return f.read()

    def __del__(self):
        self.cleanup()

    @staticmethod
    def cleanup():
      if usexml:
        # Shutdown xmlsec-crypto library
        xmlsec.cryptoShutdown()
        # Shutdown crypto library
        xmlsec.cryptoAppShutdown()
        # Shutdown xmlsec library
        xmlsec.shutdown()
        # Shutdown LibXML2
        libxml2.cleanupParser()
