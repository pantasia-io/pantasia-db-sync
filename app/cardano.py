from __future__ import annotations

import logging

from pycardano import Address
from pycardano import Network
from pycardano import VerificationKeyHash

logger = logging.getLogger('pantasia-db-sync')


def get_staking_address(address: str) -> str | None:
    # Check if address is from Shelley Era
    if address.startswith('addr'):
        # Instantiate Address object
        address_obj = Address.from_primitive(address)

        # Return staking address if staking part exists else return None
        if type(address_obj.staking_part) is VerificationKeyHash:
            return Address(
                staking_part=address_obj.staking_part,
                network=Network.MAINNET,
            ).encode()
        else:
            return None
    else:
        return None
