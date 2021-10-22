/*
 * Copyright (c) 1997-2010 Oracle and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Oracle nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import java.io.*;
import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;

/**
 
 */


public class msgsend {
    
	String BODY;
	String SBJ;
	String TO;
	

	public msgsend(String sbj, String mybody,String to) {
		BODY =  mybody;
		SBJ = sbj;
		TO = to;
	}
	
	
    
    public void send() {


        // Sender's email ID needs to be mentioned
        String from = "alessandro.blasetti@lfoundry.com";

        // Assuming you are sending email from localhost
        String host = "relay.ai.lfoundry.com";

        // Get system properties
        Properties properties = System.getProperties();

        // Setup mail server
        properties.setProperty("mail.smtp.host", host);
        properties.put("mail.smtp.port", "25"); // default port 25
        
        // Get the default Session object.
        Session session = Session.getDefaultInstance(properties);
        
        
        try {
            // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);

            // Set From: header field of the header.
            message.setFrom(new InternetAddress(from));
            

            // Set To: header field of the header.
            String[] iAdressArray = TO.split(";");
            
//            message.addRecipient(Message.RecipientType.TO, new InternetAddress("alessandro.blasetti@lfoundry.com"));
            
            for(String addr : iAdressArray)
            	message.addRecipient(Message.RecipientType.TO, new InternetAddress(addr));
            
            
//            message.addRecipient(Message.RecipientType.TO, new InternetAddress(TO));
            message.addRecipient(Message.RecipientType.CC, new InternetAddress("alessandro.blasetti@lfoundry.com"));

            // Set Subject: header field
            message.setSubject(SBJ);

            // Now set the actual message
            message.setContent(BODY,"text/html");

            // Send message
            Transport.send(message);
            System.out.println("Sent message successfully....");
            
            
            
         } catch (MessagingException mex) {
            mex.printStackTrace();
         }
    	
    }
}
