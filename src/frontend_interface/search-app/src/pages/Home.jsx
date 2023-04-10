import React, {useContext} from 'react';
import Logo from '../components/Logo/Logo';
import SearchBar from '../components/SearchBar/SearchBar';
import Button from '../components/Button/Button';
import { AppContext } from '../contexts/AppContext/AppContext';

function Home() {
    const {
        searchTerm,
        holiSearch
    } = useContext(AppContext);

    return (
        <div className='flex flex-col w-full h-screen'>
            <div className="flex flex-col items-center justify-center w-full h-full space-y-7">
                <Logo className="text-8xl" />
                <SearchBar
                className="w-5/12 h-12"
                autoFocus={ true }
                />
                <div className="space-x-3">
                    <Button
                        onClick={ () => holiSearch(searchTerm)}
                        className='w-max'
                    >
                        Search
                    </Button>
                </div>
            </div>
        </div>
    );
}


export default Home;