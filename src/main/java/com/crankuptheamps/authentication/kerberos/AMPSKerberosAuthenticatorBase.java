//////////////////////////////////////////////////////////////////////////\
//
// Copyright (c) 2012-2019 60East Technologies Inc., All Rights Reserved.
//
// This computer software is owned by 60East Technologies Inc. and is
// protected by U.S. copyright laws and other laws and by international
// treaties.  This computer software is furnished by 60East Technologies
// Inc. pursuant to a written license agreement and may be used, copied,
// transmitted, and stored only in accordance with the terms of such
// license agreement and with the inclusion of the above copyright notice.
// This computer software or any other copies thereof may not be provided
// or otherwise made available to any other person.
//
// U.S. Government Restricted Rights.  This computer software: (a) was
// developed at private expense and is in all respects the proprietary
// information of 60East Technologies Inc.; (b) was not developed with
// government funds; (c) is a trade secret of 60East Technologies Inc.
// for all purposes of the Freedom of Information Act; and (d) is a
// commercial item and thus, pursuant to Section 12.212 of the Federal
// Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
// Government's use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
////////////////////////////////////////////////////////////////////////////
//
// Vendored from https://github.com/60East/amps-authentication-java
// (AMPSKerberos/src/main/java/com/crankuptheamps/authentication/kerberos)
// to avoid an extra runtime jar on the Kafka Connect plugin path.

package com.crankuptheamps.authentication.kerberos;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crankuptheamps.client.Authenticator;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.exception.AuthenticationException;

public abstract class AMPSKerberosAuthenticatorBase implements Authenticator {

    protected String _spn;
    protected String _principalName;
    protected Base64 _base64;

    private static final Logger _logger = LoggerFactory.getLogger(AMPSKerberosAuthenticatorBase.class);

    public AMPSKerberosAuthenticatorBase(String spn_) {
        super();
        _spn = spn_;
        _base64 = new Base64();
    }

    protected abstract void init() throws AuthenticationException;

    protected abstract void dispose() throws AuthenticationException;

    public abstract String _authenticateImpl(String username_, String encodedInToken_) throws AuthenticationException;

    public String _authenticate(String username_, String encodedInToken_, boolean completing_)
            throws AuthenticationException {
        if (!completing_) {
            dispose();
            init();
        }
        return _authenticateImpl(username_, encodedInToken_);
    }

    public String authenticate(String username_, String encodedInToken_) throws AuthenticationException {
        return _authenticate(username_, encodedInToken_, false);
    }

    @Override
    public String retry(String username_, String encodedInToken_) throws AuthenticationException {
        return authenticate(username_, encodedInToken_);
    }

    @Override
    public void completed(String username_, String encodedInToken_, int reason_) throws AuthenticationException {
        if (reason_ == Message.Reason.AuthDisabled) {
            _logger.info("Authentication is disabled on the server side");
            // Calling dispose to destroy the security context and any cache credentials
            dispose();
            return;
        }
        _authenticate(username_, encodedInToken_, true);
        // Calling dispose to destroy the security context and any cache credentials
        dispose();
    }
}
