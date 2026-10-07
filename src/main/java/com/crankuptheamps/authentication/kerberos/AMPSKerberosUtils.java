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

import com.crankuptheamps.client.exception.AuthenticationException;

public class AMPSKerberosUtils {

    private static String hostPattern = "(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\\-]*[a-zA-Z0-9])";
    private static String realmPattern = "@[\\w\\d]+([\\.\\w\\d]*)?";
    private static String spnPattern = "^(\\w+/)(" + hostPattern + ")(:\\d+)?";
    private static String spnFormat = "<service>/<host>[:<port>]";
    private static String spnPatternWithRealm = String.format("%s(%s)?", spnPattern, realmPattern);
    private static String spnFormatWithRealm = "<service>/<host>[:<port>][@REALM]";

    public static void validateSPN(String spn_) throws AuthenticationException {
        if (!spn_.matches(spnPattern)) {
            throw new AuthenticationException(
                    String.format("The specified SPN %s does not match the format %s", spn_, spnFormat));
        }
    }

    public static void validateSPNWithRealm(String spn_) throws AuthenticationException {
        if (!spn_.matches(spnPatternWithRealm)) {
            throw new AuthenticationException(
                    String.format("The specified SPN %s does not match the format %s", spn_, spnFormatWithRealm));
        }
    }
}
