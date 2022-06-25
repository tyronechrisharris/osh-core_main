/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.common;

import java.security.GeneralSecurityException;
import java.util.Random;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.impl.common.IdEncoderDES;


/**
 * <p>
 * Helper class providing ID encoders for all resources available
 * on the sensor hub.
 * </p>
 *
 * @author Alex Robin
 * @since Jun 25, 2022
 */
public class IdEncoders
{
    final IdEncoder featureIdEncoder;
    final IdEncoder procIdEncoder;
    final IdEncoder sysIdEncoder;
    final IdEncoder foiIdEncoder;
    final IdEncoder dsIdEncoder;
    final IdEncoder obsIdEncoder;
    final IdEncoder csIdEncoder;
    final IdEncoder cmdIdEncoder;
    
    
    public IdEncoders(ISensorHub hub)
    {
        // TODO load seed from file
        
        // create ID encoders
        try
        {
            
            var prg = new Random(10L);
            this.featureIdEncoder = new IdEncoderDES(generateKey(prg));
            this.procIdEncoder = new IdEncoderDES(generateKey(prg));
            this.sysIdEncoder = new IdEncoderDES(generateKey(prg));
            this.foiIdEncoder = new IdEncoderDES(generateKey(prg));
            this.dsIdEncoder = new IdEncoderDES(generateKey(prg));
            this.obsIdEncoder = new IdEncoderDES(generateKey(prg));
            this.csIdEncoder = new IdEncoderDES(generateKey(prg));
            this.cmdIdEncoder = new IdEncoderDES(generateKey(prg));
        }
        catch (GeneralSecurityException e)
        {
            throw new IllegalStateException("Error generating ID encoder keys", e);
        }
        
    }
    
    
    SecretKey generateKey(Random prg) throws GeneralSecurityException
    {
        // generate 56 bytes keys from seed
        var keyBytes = new byte[56];
        prg.nextBytes(keyBytes);
        DESKeySpec dks = new DESKeySpec(keyBytes);
        var skf = SecretKeyFactory.getInstance("DES");
        return skf.generateSecret(dks);
    }
    
    
    public IdEncoder getFeatureIdEncoder()
    {
        return featureIdEncoder;
    }
    
    
    public IdEncoder getProcedureIdEncoder()
    {
        return procIdEncoder;
    }
    
    
    public IdEncoder getSystemIdEncoder()
    {
        return sysIdEncoder;
    }
    
    
    public IdEncoder getFoiIdEncoder()
    {
        return foiIdEncoder;
    }
    
    
    public IdEncoder getDataStreamIdEncoder()
    {
        return dsIdEncoder;
    }
    
    
    public IdEncoder getObsIdEncoder()
    {
        return foiIdEncoder;
    }
    
    
    public IdEncoder getCommandStreamIdEncoder()
    {
        return csIdEncoder;
    }
    
    
    public IdEncoder getCommandIdEncoder()
    {
        return cmdIdEncoder;
    }
}
