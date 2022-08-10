from __future__ import annotations

from pycardano import Address
from pycardano import Network


def get_staking_address(address):
    # Check if address is from Shelley Era
    if address.startswith('addr'):
        # Instantiate Address object
        address_obj = Address.from_primitive(address)

        # Return staking address if staking part exists else return None
        if address_obj.staking_part is None:
            return None
        else:
            return Address(
                staking_part=address_obj.staking_part,
                network=Network.MAINNET,
            ).encode()
    else:
        return None
