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

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crankuptheamps.client.exception.AuthenticationException;
import com.sun.security.auth.callback.TextCallbackHandler;

public class AMPSKerberosGSSAPIAuthenticator extends AMPSKerberosAuthenticatorBase {
    private GSSContext _secContext;
    private int _lifetime;
    private String _loginContextName;

    private static final Logger _logger = LoggerFactory.getLogger(AMPSKerberosGSSAPIAuthenticator.class);

    public AMPSKerberosGSSAPIAuthenticator(String spn_, String loginContextName_, int lifetime_)
            throws AuthenticationException {
        super(spn_);
        AMPSKerberosUtils.validateSPN(spn_);
        _spn = _spn.replaceAll("/", "@");
        _loginContextName = loginContextName_;
        _lifetime = lifetime_;
    }

    public AMPSKerberosGSSAPIAuthenticator(String spn_, String loginContextName_) throws AuthenticationException {
        this(spn_, loginContextName_, 8 * 3600);
    }

    protected void init() throws AuthenticationException {
        try {
            LoginContext loginContext = new LoginContext(_loginContextName, new TextCallbackHandler());
            loginContext.login();
            Subject subject = loginContext.getSubject();
            Principal principal = subject.getPrincipals().iterator().next();
            _principalName = principal.getName();

            Subject.doAs(subject, new java.security.PrivilegedExceptionAction<Object>() {
                public Object run() throws IOException, AuthenticationException {
                    try {
                        acquireCredentials();
                    } catch (AuthenticationException e) {
                        throw new AuthenticationException(e);
                    }
                    return null;
                }
            });
        } catch (LoginException | PrivilegedActionException e) {
            throw new AuthenticationException(e);
        }
    }

    private void acquireCredentials() throws AuthenticationException {
        try {
            GSSManager manager = GSSManager.getInstance();
            _logger.info("Acquiring kerberos credentials for user {} connecting to service {}", _principalName, _spn);
            GSSName clientName = manager.createName(_principalName, GSSName.NT_USER_NAME);
            GSSCredential clientCreds = manager.createCredential(clientName, _lifetime, (Oid[]) null,
                    GSSCredential.INITIATE_ONLY);

            GSSName peerName = manager.createName(_spn, GSSName.NT_HOSTBASED_SERVICE);
            _secContext = manager.createContext(peerName, null, clientCreds, GSSContext.DEFAULT_LIFETIME);
            _secContext.requestMutualAuth(true);
        } catch (GSSException e) {
            throw new AuthenticationException(e);
        }
    }

    private byte[] initializeSecurityContext(byte[] inToken_) throws AuthenticationException {
        try {
            return _secContext.initSecContext(inToken_, 0, inToken_.length);
        } catch (GSSException e) {
            throw new AuthenticationException(e);
        }
    }

    @Override
    public String _authenticateImpl(String username_, String encodedInToken_) throws AuthenticationException {
        byte[] inToken;
        if (encodedInToken_ == null) {
            _logger.info("Initializing kerberos security context for user {} connecting to service {}", _principalName,
                    _spn);
            inToken = new byte[0];
        } else {
            _logger.info("Finalizing kerberos authentication for user {} connecting to service {}", _principalName,
                    _spn);
            inToken = _base64.decode(encodedInToken_);
        }
        byte[] outToken = initializeSecurityContext(inToken);
        return (outToken == null) ? "" : new String(_base64.encode(outToken));
    }

    @Override
    public void dispose() throws AuthenticationException {
        if (_secContext != null) {
            try {
                _secContext.dispose();
                _secContext = null;
            } catch (GSSException e) {
                throw new AuthenticationException(e);
            }
        }
    }
}
