import asyncio
import csv
import json
import random
import time

from eth_utils import to_hex
from web3.eth import AsyncEth
from web3 import Web3
from loguru import logger
import aiohttp

from info import (claim_abi, claim_address,
                  arkham_abi, arkham_token)
from config import gwei, amount_wallets_in_batch


class Help:
    async def check_status_tx(self, tx_hash, ):
        logger.info(
            f'{self.address} - жду подтверждения транзакции https://etherscan.io/tx/{self.w3.to_hex(tx_hash)}...')

        start_time = int(time.time())
        while True:
            current_time = int(time.time())
            if current_time >= start_time + 150:
                logger.info(
                    f'{self.address} - транзакция не подтвердилась за 150 cекунд, начинаю повторную отправку...')
                return 0
            try:
                status = (await self.w3.eth.get_transaction_receipt(tx_hash))['status']
                if status == 1:
                    return status
                await asyncio.sleep(1)
            except Exception as error:
                await asyncio.sleep(1)

    async def sleep_indicator(self, secs):
        logger.info(f'{self.address} - жду {secs} секунд...')
        await asyncio.sleep(secs)

class Claim(Help):
    def __init__(self, acc_info, proxy=None):
        self.w3 = Web3(Web3.AsyncHTTPProvider('https://eth.llamarpc.com'),
                       modules={'eth': (AsyncEth,)}, middlewares=[])

        self.proxy = f'http://{proxy}' if proxy else None
        self.acc_info = acc_info
        self.account = self.w3.eth.account.from_key(self.acc_info.split(':')[2])
        self.address = self.account.address

    async def auth(self):
        log = self.acc_info.split(':')[0]
        pass_ = self.acc_info.split(':')[1]
        errors = 0
        params = {
            'key': 'AIzaSyA9EERCXQ0gQstZRwcQ_Ws8XAELd2FUaXM',
        }

        json_data = {
            'email': log,
            'password': pass_ ,
            'returnSecureToken': True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post('https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyPassword', params=params, json=json_data,
                                        proxy=self.proxy) as response:
                    if response.status == 200:
                        token = json.loads(await response.text())['idToken']
                        logger.success(f'{self.address} - Успешно вошёл в аккаунт...')
                        return token
                    logger.error(f'{self.address} - Не смог войти в аккаунт...')
                    errors += 1
                    if errors == 5:
                        return False
                    return await self.auth()
        except Exception as e:
            logger.error(f'{self.address} - Ошибка при входе в аккаунт - {e}...')
            await asyncio.sleep(1)
            return False

    async def get_proof(self):
        token = await self.auth()
        if not token:
            return False
        headers = {'authorization': token}
        errors = 0
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://api.arkhamintelligence.com/user/airdrop/claim', headers=headers,
                                        proxy=self.proxy) as response:
                    if response.status == 200:
                        data = json.loads(await response.text())
                        logger.success(f'{self.address} - успешно получил proof для клейма...')
                        return data
                    logger.error(f'{self.address} - не смог получить proof для клейма...')
                    errors += 1
                    if errors == 5:
                        return False
                    return await self.auth()
        except Exception as e:
            logger.error(f'{self.address} - ошибка при получении пруфа{e}...')
            await asyncio.sleep(1)
            return False

    async def check_gas(self):
        while True:
            try:
                gas = await self.w3.eth.gas_price
                gas_ = self.w3.from_wei(gas, 'gwei')
                logger.success(f'gwei сейчас - {gas_}...')
                if gwei > gas_:
                    return True
                logger.error(f'gwei слишком большой, жду понижения...')
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(e)
                await asyncio.sleep(2)
                return await self.check_gas()

    async def claim(self):
        data = await self.get_proof()
        if not data:
            return False

        if 'claimedHash' in data.keys():
            logger.error(f'{self.address} - уже заклеймлено!...')
            return False
        claim_ = self.w3.eth.contract(address=Web3.to_checksum_address(claim_address), abi=claim_abi)
        try:
            proof = data['proof']
            amount = int(data['amount'])

            tx = await claim_.functions.claim(self.address, amount, proof).build_transaction({
                'from': self.address,
                'nonce': await self.w3.eth.get_transaction_count(self.address),
                'maxFeePerGas': int(await self.w3.eth.gas_price),
                'maxPriorityFeePerGas': int(await self.w3.eth.gas_price*0.8)
            })
            gas = await self.w3.eth.gas_price
            tx['maxFeePerGas'] = gas
            tx['maxPriorityFeePerGas'] = gas
            sign = self.account.sign_transaction(tx)
            hash_ = await self.w3.eth.send_raw_transaction(sign.rawTransaction)
            status = await self.check_status_tx(hash_)
            if status:
                logger.success(
                    f'{self.address} - успешно заклеймил {amount/10**18} ARKM https://etherscan.io/tx/{to_hex(hash_)}...')
                return True
        except Exception as e:
            error = str(e)
            if 'insufficient funds' in error or 'insufficient funds for gas' in error or 'gas required exceeds allowance' in error:
                logger.error(f'{self.address} - не хватает денег на газ, заканчиваю работу через 5 секунд...')
                await asyncio.sleep(5)
                return False
            if 'Airdrop: Invalid Proof' in error:
                true_address = str(data['address'])
                logger.error(f'{self.address} - неправильно подставлен приватный ключ для клейма, истинный адресс - {true_address}...')
                return False
            else:
                logger.error(f'{self.address} - {e}...')
                return False

    async def balance(self):
        try:
            contract = self.w3.eth.contract(Web3.to_checksum_address(arkham_token), abi=arkham_abi)
            balance = await contract.functions.balanceOf(Web3.to_checksum_address(self.address)).call()
            return balance
        except Exception as e:
            await asyncio.sleep(1)


    async def send(self):
        contract = self.w3.eth.contract(Web3.to_checksum_address(arkham_token), abi=arkham_abi)
        to = self.acc_info.split(':')[3]
        gas = await self.check_gas()
        while True:
            balance = await self.balance()
            if balance > 0:
                try:
                    logger.info(f'{self.address} - отправляю токены на {to}...')
                    tx = await contract.functions.transfer(Web3.to_checksum_address(to), balance).build_transaction({
                        'from': self.address,
                    'nonce': await self.w3.eth.get_transaction_count(self.address),
                    'maxFeePerGas': int(await self.w3.eth.gas_price),
                    'maxPriorityFeePerGas': int(await self.w3.eth.gas_price*0.8)
                    })
                    gas = await self.w3.eth.gas_price
                    tx['maxFeePerGas'] = gas
                    tx['maxPriorityFeePerGas'] = gas
                    sign = self.account.sign_transaction(tx)
                    hash_ = await self.w3.eth.send_raw_transaction(sign.rawTransaction)
                    status = await self.check_status_tx(hash_)
                    if status:
                        logger.success(
                            f'{self.address} - успешно отправил {balance / 10 ** 18} ARKM https://etherscan.io/tx/{to_hex(hash_)}...')
                        return self.address, True
                except Exception as e:
                    error = str(e)
                    if 'insufficient funds' in error or 'insufficient funds for gas' in error or 'gas required exceeds allowance':
                        logger.error(f'{self.address} - не хватает денег на газ, заканчиваю работу через 5 секунд...')
                        await asyncio.sleep(5)
                        return self.address, False
                    else:
                        logger.error(f'{self.address} - {e}...')
                        return self.address, False
            else:
                logger.info(f'{self.address} - нет баланса ARKM иду клеймить...')
                res = await self.claim()
                if not res:
                    return self.address, 'нечего клеймить'

async def write_to_csv(adress, res):
    with open('result.csv', 'a', newline='') as file:
        writer = csv.writer(file)

        if file.tell() == 0:
            writer.writerow(['address','result'])

        writer.writerow([adress, res])


async def main():
    with open("accs.txt", "r") as f:
        accs = [row.strip() for row in f]
    random.shuffle(accs)
    logger.info(f'Начинаю работу на {len(accs)} кошельках...')
    batches = [accs[i:i + amount_wallets_in_batch] for i in range(0, len(accs), amount_wallets_in_batch)]

    tasks = []
    for batch in batches:
        for acc in batch:
            claim_ = Claim(acc)
            tasks.append(claim_.send())
        res = await asyncio.gather(*tasks)

        for res_ in res:
            address, result = res_
            await write_to_csv(address, result)


        tasks = []

    logger.success(f'Успешно сделал {len(accs)} кошельков...')
    logger.success(f'muнетинг закончен...')




if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())

